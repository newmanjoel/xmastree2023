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


@lru_cache(maxsize=16777216, typed=False)
def grb_to_int(g: int, r: int, b: int) -> int:
    return int((r << 16) | (g << 8) | b)


def convert_row_to_color(
    input_row: list[int], number_of_columns: int = 1500
) -> list[int]:
    return_list = [0] * (number_of_columns // 3)
    for pixel_num in range(0, number_of_columns, 3):
        led_pixel_index = pixel_num // 3
        led_pixel_color = grb_to_int(
            input_row[pixel_num], input_row[pixel_num + 1], input_row[pixel_num + 2]
        )
        return_list[led_pixel_index] = led_pixel_color
    return return_list


def sanitize_column_names(input_df: pd.DataFrame) -> pd.DataFrame:
    return_df = input_df.copy(deep=True)

    def is_matching_pattern(s):
        pattern = re.compile(r"^[a-zA-Z]_\d+$")
        return bool(pattern.match(s))

    for name in return_df.columns:
        if not is_matching_pattern(name):
            return_df.drop(name, axis=1, inplace=True)
    return return_df


def convert_df_to_list_of_int_speedy(input_df: pd.DataFrame) -> list[list[int]]:
    local_logger = logger.getChild("df_2_int")
    local_logger.debug("starting conversion")
    start_time = time.time()
    working_df = input_df.copy(deep=True)
    time_2 = time.time()
    working_df = sanitize_column_names(working_df)
    working_df.reindex(column_names, axis=1)
    time_3 = time.time()
    raw_data = working_df.to_numpy(dtype=np.ubyte)
    raw_data = raw_data.astype(dtype=np.ubyte)
    time_4 = time.time()

    results = np.apply_along_axis(convert_row_to_color, 1, raw_data)
    returned_list = results.tolist()
    end_time = time.time()

    copy_time = time_2 - start_time
    clean_time = time_3 - time_2
    unit_change_time = time_4 - time_3
    enumerate_time = end_time - time_4
    total_time = end_time - start_time

    # Benchmark
    # copy:0.01650 clean:0.04447 types:0.00295 looping:7.64509 total:7.70900
    # after cashing the grb_to_int function
    # copy:0.01680 clean:0.04479 types:0.00313 looping:3.85402 total:3.91874
    # after using numpy apply along axis
    # copy:0.01663 clean:0.04498 types:0.00311 looping:11.00467 total:11.06938
    # doubling down on numpy apply along axis
    # copy:0.01734 clean:0.04529 types:0.00298 looping:10.99190 total:11.05752
    # using np.apply_+along_axis for rows and cashed looping ints
    # copy:0.01702 clean:0.04490 types:0.00296 looping:4.00124 total:4.06612
    # using np.apply_along_axis for frames and looping for rows and casheing all the colors
    # copy:0.01617 clean:0.04324 types:0.00275 looping:2.50638 total:2.56854

    # using the np.apply_along_axis for rows and cashed looping ints as that seems to cleanest/fastest combo

    local_logger.debug(
        f"copy:{copy_time:0.5f} clean:{clean_time:0.5f} types:{unit_change_time:0.5f} looping:{enumerate_time:0.5f} total:{total_time:0.5f}"
    )

    return returned_list


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

def inert_dataframe_into_database(conn:sqlite3.Connection, dataframe: pd.DataFrame, file_id:int) -> None:
    local_logger = logger.getChild("insert_dataframe_into_database")
    data_cursor = conn.cursor()
    pre_split_columns = []
    for i in range(1,dataframe.shape[1], 3):
        pre_split_columns.append(dataframe.columns[i:i+3])

    start_database_entry = time.time()
    speedy_data = dataframe.to_numpy(dtype=np.ubyte)

    total_rows = len(speedy_data)
    total_columns = len(speedy_data[0])

    local_logger.debug(f"{total_rows=} {total_columns=}")

    data_to_injest = []
    for row_index, row in enumerate(speedy_data):
        percent = float(row_index) / total_rows * 100
        # local_logger.debug(f"{percent:0.2f}%")
        led_number = 0
        # local_logger.debug(f"{row=}")
        for columns in range(1, total_columns - 1, 3):
            # local_logger.debug(f"{columns=}")
            data_to_injest.append(
                (
                    file_id,
                    row_index,
                    led_number,
                    row[columns],
                    row[columns + 1],
                    row[columns + 2],
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


def append_database_from_csv(conn : sqlite3.Connection, csv_path: str, overwrite_if_already_injested:bool = False) -> bool:

    file_already_injested = check_if_file_already_injested(conn,csv_path)
    if file_already_injested and not overwrite_if_already_injested:
        logger.getChild("append_database_from_csv").info(f"{file_already_injested=}")
        return False

    data = load_csv(csv_path)

    file_cursor = conn.cursor()
    file_cursor.execute("INSERT INTO `files` (`filename`,`frames`) VALUES (?,?)", (str(csv_path),len(data)))
    file_id: int = file_cursor.lastrowid  # type: ignore
    logger.getChild("append_database_from_csv").debug(f"csv contents: {data.shape=} file_id that was added {file_id=}")

    inert_dataframe_into_database(conn, data, file_id)

    conn.commit()
    # conn.rollback()
    return True

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
            logger.getChild("import_all_csv_from_folder").warn(f"Garbage collector: collected {collected} objects")
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
    conn.commit()
