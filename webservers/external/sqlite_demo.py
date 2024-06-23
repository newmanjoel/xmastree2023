import sqlite3
from pathlib import Path
import pandas as pd
from numpy import ubyte
import time
import logging
import colorlog



def setup_common_logger(logger: logging.Logger) -> logging.Logger:
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    color_formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
        },
    )
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    return logger



logger = setup_common_logger(logging.getLogger("sqlite_demo"))

def create_default_table_structure(conn: sqlite3.Connection) -> None:
    '''creates the default table structure. Does NOT commit the change.'''
    cursor = conn.cursor()
    # Create the 'files' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT NOT NULL,
        frames INTEGER NOT NULL
    )
    ''')

    # Create the 'rgb_values' table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rgb_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL,
        frame INTEGER NOT NULL,
        led INTEGER NOT NULL,
        red INTEGER NOT NULL,
        green INTEGER NOT NULL,
        blue INTEGER NOT NULL,
        FOREIGN KEY (file_id) REFERENCES files (id)
    )
    ''')




def create_and_save_database(db_name: str | Path) -> None:
    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    create_default_table_structure(conn)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    logger.getChild("create_and_save_database").debug(f"Database '{db_name}' created and saved successfully.")
    

def display_all_files_in_db(conn: sqlite3.Connection) -> None:
    local_logger = logger.getChild("display.files")
    cursor = conn.cursor()
    cursor.execute('''SELECT * FROM files''')
    results = cursor.fetchall()
    local_logger.info(results)


def load_and_return_database(db_name:str|Path) -> sqlite3.Connection:
    # Connect to the existing database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Example query to verify the tables exist and load data
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    logger.getChild("load_and_return_database").debug(f"Tables in database '{db_name}': {tables}")

    return conn
    
def check_if_file_already_injested(conn:sqlite3.Connection, file_path: str) -> bool:
    cursor = conn.cursor()
    cursor.execute('''
    SELECT 
        `filename`
    FROM
        `files`
    WHERE
        `filename`=?
    ''', (file_path,))
    results = cursor.fetchall()
    return len(results)>0

def load_csv(csv_path:str) -> pd.DataFrame:
    start = time.time()
    csv = pd.read_csv(csv_path)
    csv = csv.astype(ubyte)
    end = time.time()
    logger.getChild("load_csv").debug(f"Loading file took {end-start:0.3f}s")
    return csv

def inert_dataframe_into_database(conn:sqlite3.Connection, dataframe: pd.DataFrame, file_id:int) -> None:
    local_logger = logger.getChild("insert_dataframe_into_database")
    data_cursor = conn.cursor()
    pre_split_columns = []
    for i in range(1,dataframe.shape[1], 3):
        pre_split_columns.append(dataframe.columns[i:i+3])
    
    local_logger.debug(f"{dataframe.shape=}")

    start_database_entry = time.time()

    data_to_injest = []
    # FRAME_ID R_0 G_0 B_0 R_1 ->
    for index, row in dataframe.iterrows():
        for led_number, col in enumerate(pre_split_columns):
            data_to_injest.append((file_id, index, led_number, *row[col]))
    injest_time_end = time.time()
    #for data in data_to_injest:
    #    (file_id,index,led_number,row) = data
    #    data_cursor.execute("INSERT INTO `rgb_values` (`file_id`, `frame`, `led`, `red`, `green`, `blue`) VALUES(?,?,?,?,?,?)",(file_id, index, led_number, *row) )
    data_cursor.executemany("INSERT INTO `rgb_values` (`file_id`, `frame`, `led`, `red`, `green`, `blue`) VALUES(?,?,?,?,?,?)", data_to_injest)

    # executemany takes 79.3s
    # execute takes 52.8s

    end_database_entry = time.time()
    injest_time = injest_time_end-start_database_entry
    add_database_time = end_database_entry-start_database_entry
    time_per_line = add_database_time/dataframe.shape[0]
    local_logger.debug(f"adding to injest array took {injest_time:0.3f}s")
    local_logger.debug(f"adding to database took {add_database_time:0.3f}s")
    local_logger.debug(f"time per line {time_per_line:0.3f}s")


def append_database_from_csv(conn : sqlite3.Connection, csv_path: str, overwrite_if_already_injested:bool = False) -> bool:
    
    file_already_injested = check_if_file_already_injested(conn,csv_path)
    if file_already_injested and not overwrite_if_already_injested:
        logger.getChild("append_database_from_csv").info(f"{file_already_injested=}")
        return False

    
    data = load_csv(csv_path)

    file_cursor = conn.cursor()
    file_cursor.execute("INSERT INTO `files` (`filename`,`frames`) VALUES (?,?)", (str(csv_path),len(data)))
    file_id = file_cursor.lastrowid
    logger.getChild("append_database_from_csv").debug(f"csv contents: {data.shape=} file_id that was added {file_id=}")

    inert_dataframe_into_database(conn, data, file_id)

    conn.commit()
    # conn.rollback()
    return True


def get_view_in_conn(conn: sqlite3.Connection) -> None:
    '''Get a view into the database for specific file id and in the right format for displaying'''
    get_view_start = time.time()
    cursor = conn.cursor()
    cursor.execute('SELECT `red`,`green`,`blue` FROM rgb_values WHERE file_id is 1 ORDER BY led,frame')
    # Fetch all results
    results = cursor.fetchall()
    get_view_end = time.time()
    #logger.getChild("get_view_in_conn").info(results)
    logger.getChild("get_view_in_conn").info(f"Results time {get_view_end-get_view_start:0.3f}s")

def import_all_csv_from_folder(conn: sqlite3.Connection, folder:Path) -> None:
    import gc
    import_time_start = time.time()
    for file in list(folder.glob("*.csv")):
        logger.getChild("import_all_csv_from_folder").info(f"injesting: {file.name}")
        file_time_start = time.time()
        append_database_from_csv(conn, str(file), overwrite_if_already_injested=False)
        file_time_end = time.time()
        logger.getChild("import_all_csv_from_folder").info(f"injested: {file.name}| took {file_time_end-file_time_start:0.3f}s")
        collected = gc.collect()
        if collected > 0:
            logger.getChild("import_all_csv_from_folder").warn(f"Garbage collector: collected {collected} objects")
    import_time_end = time.time()
    logger.getChild("import_all_csv_from_folder").info(f"Results time {import_time_end-import_time_start:0.3f}s")
        
            

if __name__=="__main__":

    # Example usage
    db_name = Path('files_and_rgb.db') 
    logger.debug(f"Current working directory is {Path('.').absolute()}")
    example_csv = Path(r"./examples/pulsing_heart.csv")
    logger.info(f"Database location {db_name.absolute()}")
    # db_name = ":memory:"
    create_and_save_database(db_name)
    
    with load_and_return_database(db_name) as conn:
        # create_default_table_structure(conn)
        display_all_files_in_db(conn)
        import_all_csv_from_folder(conn, Path(r"examples/"))
        #append_database_from_csv(conn, str(example_csv), overwrite_if_already_injested=False)
        # get_view_in_conn(conn)
        conn.commit()
        
        get_view_in_conn(conn)


