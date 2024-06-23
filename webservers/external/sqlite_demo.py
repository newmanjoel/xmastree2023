from functools import lru_cache
import re
import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
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


@lru_cache(maxsize=2)
def all_standard_column_names(num: int) -> list[str]:
    results = []
    for i in range(num):
        results.append(f"R_{i}")
        results.append(f"G_{i}")
        results.append(f"B_{i}")
    return results


column_names = all_standard_column_names(500)


def create_and_save_database(db_name: str|Path):
    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_name)
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
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS rgb_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL,
        frame INTEGER NOT NULL,
        led INTEGER NOT NULL,
        red INTEGER NOT NULL,
        green INTEGER NOT NULL,
        blue INTEGER NOT NULL,
        FOREIGN KEY (file_id) REFERENCES files (id)
    ) STRICT
    """
    )

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    logger.getChild("create_and_save_database").debug(f"Database '{db_name}' created and saved successfully.")


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


def insert_dataframe_into_database(
    conn: sqlite3.Connection, dataframe: pd.DataFrame, file_id: int
) -> None:
    local_logger = logger.getChild("insert_dataframe_into_database")
    data_cursor = conn.cursor()

    start_database_entry = time.time()
    speedy_data = dataframe.to_numpy()

    total_rows = len(speedy_data)
    total_columns = len(speedy_data[0])

    local_logger.debug(f"{total_rows=} {total_columns=}")

    data_to_injest = []
    for row_index, row in enumerate(speedy_data):
        led_number = 0
        for columns in range(1, total_columns - 1, 3):
            data_to_injest.append(
                (
                    int(file_id),
                    int(row_index),
                    int(led_number),
                    int(row[columns]),
                    int(row[columns + 1]),
                    int(row[columns + 2]),
                )
            )
            led_number += 1

    injest_time_end = time.time()
    logger.getChild("insert_dataframe_into_database").debug(
        f"adding to injest array took {injest_time_end-start_database_entry:0.3f}s"
    )
    # for data in data_to_injest:
    # data_cursor.execute("INSERT INTO `rgb_values` (`file_id`, `frame`, `led`, `red`, `green`, `blue`) VALUES(?,?,?,?,?,?)",(file_id, index, led_number, *row[col]) )
    data_cursor.executemany("INSERT INTO `rgb_values` (`file_id`, `frame`, `led`, `red`, `green`, `blue`) VALUES(?,?,?,?,?,?)",data_to_injest)

    # executemany takes 79.3s
    # execute takes 52.8s

    end_database_entry = time.time()
    logger.getChild("insert_dataframe_into_database").debug(f"adding to database took {end_database_entry-start_database_entry:0.3f}s")


def append_database_from_csv(
    conn: sqlite3.Connection, csv_path: str, overwrite_if_already_injested: bool = False
) -> bool:
    file_already_injested = check_if_file_already_injested(conn,csv_path)
    if file_already_injested and not overwrite_if_already_injested:
        logger.getChild("append_database_from_csv").info(f"{file_already_injested=}")
        return False

    data = load_csv(csv_path)

    file_cursor = conn.cursor()
    file_cursor.execute("INSERT INTO `files` (`filename`,`frames`) VALUES (?,?)", (str(csv_path),len(data)))
    file_id: int = file_cursor.lastrowid  # type: ignore
    logger.getChild("append_database_from_csv").debug(f"csv contents: {data.shape=} file_id that was added {file_id=}")

    insert_dataframe_into_database(conn, data, file_id)

    conn.commit()
    # conn.rollback()
    return True


def export_file_to_csv(conn: sqlite3.Connection, file_path: Path, file_id: int) -> Path:
    if type(file_path) is str:
        file_path = Path(file_path)
    file_path.touch(exist_ok=True)

    cursor = conn.cursor()
    cursor.execute("SELECT `id` FROM `files` WHERE `id`=?", (file_id,))
    results = cursor.fetchall()
    if len(results) == 0:
        raise ValueError(f"{file_id=} not found in the sqlite database")
    cursor.execute("SELECT `frames` FROM `files` WHERE `id`=?", (file_id,))
    frame_num = cursor.fetchall()
    if len(frame_num) != 1:
        raise ValueError(
            f"{frame_num=} should be a single number where the {len(frame_num)=}"
        )
    logger.getChild("export_test").debug(f"{frame_num=}")
    frame_num = int(frame_num[0][0])  # type: ignore

    # query = f"SELECT `frame`,`led`, `red`,`green`,`blue` FROM rgb_values WHERE file_id is {file_id} ORDER BY frame,led"
    # df = pd.read_sql_query(query, conn)

    with open(file_path, "w") as fo:

        for frame_id in range(0, frame_num):
            # each row is tuple containing the RGB values for a single LED
            row_str = f"{frame_id}"
            cursor.execute(
                "SELECT `frame`,`led`, `red`,`green`,`blue` FROM rgb_values WHERE file_id is ? AND frame is ? ORDER BY frame,led",
                (file_id, frame_id),
            )
            results = cursor.fetchall()
            # results should be 500 rows by 5 columns
            local_row = 0.0
            for row_results in results:
                # logger.getChild("export_test").debug(f"{local_row/500 *100}%")
                row_str += f", {row_results[2]}, {row_results[3]}, {row_results[4]}"
                local_row += 1.0
            # append the row to the file
            row_str = row_str + "\n"
            fo.write(row_str)
            logger.getChild("export_test").debug(f"{frame_id=}")

    # df = pd.DataFrame(results)
    # df = df.astype(np.int8)
    # df.to_csv(file_path, index=False)

    return file_path


def get_view_in_conn(conn: sqlite3.Connection) -> None:
    get_view_start = time.time()
    cursor = conn.cursor()
    cursor.execute('SELECT `red`,`green`,`blue` FROM rgb_values WHERE frame is 0 AND file_id is 3 ORDER BY led')
    # Fetch all results
    results = cursor.fetchall()
    get_view_end = time.time()
    logger.getChild("get_view_in_conn").info(results)
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
            logger.getChild("import_all_csv_from_folder").warning(
                f"Garbage collector: collected {collected} objects"
            )
    import_time_end = time.time()
    logger.getChild("import_all_csv_from_folder").info(
        f"Results time {import_time_end-import_time_start:0.3f}s"
    )


# Bouncy-ball.csv took 72.676 seconds to be injested
# chasing_lights.csv took 35.470 seconds to be injested
db_name = Path(
    r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\webservers\files_and_rgb.db"
)
# example_csv = Path(r"/Users/joelnewman/GitHub/QuickPython/pulsing_heart.csv")
logger.info(f"Database location {db_name.absolute()}")
create_and_save_database(db_name)
with load_and_return_database(db_name) as conn:
    import_all_csv_from_folder(
        conn, Path(r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\examples")
    )
    # append_database_from_csv(conn, str(example_csv), overwrite_if_already_injested=True)
    # get_view_in_conn(conn)
    try:
        output_file = export_file_to_csv(conn, Path(r"test_export_csv"), 17)
        logger.info(f"{output_file.absolute()=}")
    except ValueError as e:
        logger.warning(e)
