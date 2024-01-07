import queue
import logging
import pandas as pd
from pathlib import Path
import time


import os, sys

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)


from common.common_objects import setup_common_logger, all_standard_column_names
from settings_manager import SettingsManager

logger = logging.getLogger("web_commands")
logger = setup_common_logger(logger)

settings_manager = SettingsManager()
logger.info(f"{settings_manager.get_setting('fps')=}")


led_num = 500

column_names = all_standard_column_names(led_num)


def setup_global_vars() -> None:
    pass


def handle_fill(args, queue: queue.Queue):
    # converts RGB into a GRB hex
    if type(args) != list:
        logger.getChild("fill").error(
            f"trying to fill with something that is not a list {type(args)=}\n{args=}"
        )
        return
    if len(args) != 3:
        logger.getChild("fill").error(
            f"trying to fill with more than 3 elements {len(args)=}\n{args=}"
        )
        return
    color_r = int(args[0])
    color_g = int(args[1])
    color_b = int(args[2])
    data = [color_r, color_g, color_b] * led_num

    current_df_sequence = pd.DataFrame([data], index=range(1), columns=column_names)
    queue.put(current_df_sequence)


def handle_one(args, queue: queue.Queue):
    # converts RGB into a GRB hex
    if type(args) != list:
        logger.getChild("fill").error(
            f"trying to fill with something that is not a list {type(args)=}\n{args=}"
        )
        return
    if len(args) != 4:
        logger.getChild("fill").error(
            f"trying to fill with more than 3 elements {len(args)=}\n{args=}"
        )
        return
    index = int(args[0])
    color_r = int(args[1])
    color_g = int(args[2])
    color_b = int(args[3])

    data = [0, 0, 0] * led_num
    data[index] = color_r
    data[index + 1] = color_g
    data[index + 2] = color_b

    current_df_sequence = pd.DataFrame([data], index=range(1), columns=column_names)
    queue.put(current_df_sequence)


def handle_file(args, queue: queue.Queue):
    # load a csv file
    # load that into a dataframe
    # check that it has the right size
    # check that each element is a hex code
    local_logger = logger.getChild("handle_file")

    if type(args) != str:
        local_logger.error(f"needed a file path, got {type(args)=}, {args=}")
        return
    file_path = Path(args)
    if not file_path.exists():
        local_logger.error(f"File dosn't exist. {file_path=}")
        return

    results = None

    start = time.time()
    results = pd.read_csv(file_path)
    end = time.time()
    local_logger.debug(f"loaded the file to a dataframe and it took {end-start:0.3f}s")

    local_logger.debug(
        f"loaded the file to a dataframe and it is using {results.memory_usage(deep=True).sum()}b"
    )

    current_df_sequence = results
    queue.put(current_df_sequence)
