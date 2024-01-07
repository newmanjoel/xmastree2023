import queue
import logging
import pandas as pd
import threading
import socket
import json
from pathlib import Path
import time

# networking imports
import socket
import select
import json

import os, sys

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)

import common.common_send_recv as common_send_recv
from common.common_objects import (
    setup_common_logger,
    all_standard_column_names,
    log_when_functions_start_and_stop,
)
from common.common_send_recv import send_message, receive_message
import config

logger = logging.getLogger("commands")
logger = setup_common_logger(logger)


logger.info(f"{config.fps=}")


column_names = all_standard_column_names(config.led_num)


def handle_get_logs(*, sock: socket.socket, **kwargs):
    send_message(sock, json.dumps(config.log_capture.getvalue()).encode("utf-8"))


def handle_fps(*, value: float, **kwargs) -> None:
    config.fps = float(value)


def handle_brightness(*, value: float, **kwargs) -> None:
    config.pixels.brightness = float(value)  # type: ignore


def handle_getting_temp(*, sock: socket.socket, **kwargs) -> None:
    """measure the temperature of the raspberry pi"""
    import subprocess

    result = subprocess.run(
        ["vcgencmd", "measure_temp"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    json_string = json.dumps({"temp": str(result.stdout)})
    send_message(sock, json_string.encode("utf-8"))
    logger.getChild("temp").debug(f"Sent back {json_string}")


def handle_fill(*, value: list[int], display_queue: queue.Queue, **kwargs):
    # converts RGB into a GRB hex
    if type(value) != list:
        logger.getChild("fill").error(
            f"trying to fill with something that is not a list {type(value)=}\n{value=}"
        )
        return
    if len(value) != 3:
        logger.getChild("fill").error(
            f"trying to fill with more than 3 elements {len(value)=}\n{value=}"
        )
        return
    color_r = int(value[0])
    color_g = int(value[1])
    color_b = int(value[2])
    data = [color_r, color_g, color_b] * config.led_num

    current_df_sequence = pd.DataFrame([data], index=range(1), columns=column_names)
    display_queue.put(current_df_sequence)


def handle_one(*, value: list[int], display_queue: queue.Queue, **kwargs):
    # converts RGB into a GRB hex
    if type(value) != list:
        logger.getChild("set_one").error(
            f"trying to fill with something that is not a list {type(value)=}\n{value=}"
        )
        return
    if len(value) != 4:
        logger.getChild("set_one").error(
            f"trying you need 4 elements to set a specific led {len(value)=}\n{value=}"
        )
        return
    index = int(value[0])
    color_r = int(value[1])
    color_g = int(value[2])
    color_b = int(value[3])

    data = [0, 0, 0] * config.led_num
    data[index] = color_r
    data[index + 1] = color_g
    data[index + 2] = color_b

    current_df_sequence = pd.DataFrame([data], index=range(1), columns=column_names)
    display_queue.put(current_df_sequence)


def handle_getting_list_of_files(
    *, send_queue: queue.Queue, sock: socket.socket, **kwargs
) -> None:
    """Return a list of the current CSV's that can be played"""
    csv_file_path = Path("/home/pi/github/xmastree2023/examples")
    csv_files = list(map(str, list(csv_file_path.glob("*.csv"))))
    data = json.dumps(csv_files).encode("utf-8")
    send_queue.put((sock, data))
    # send_message(sock, data)


def handle_add_list(*, value: list[int], display_queue: queue.Queue, **kwargs) -> None:
    if type(value) == list:
        pass
    else:
        logger.getChild("add_list").warning(
            f"needed a list, but got {type(value)} of {value=}"
        )
        return

    # going to assume this is in order
    # note that the rows and columns are one based and not zero based
    working_df: pd.DataFrame = config.current_dataframe  # type: ignore
    if "FRAME_ID" in working_df.columns:
        working_df = working_df.drop("FRAME_ID", axis=1)

    current_row, current_column = working_df.shape

    if len(value) != current_column:
        logger.getChild("add_list").warning(
            f"needed a list of len({current_column}), but got {len(value)} of {value=}"
        )
        return

    config.current_dataframe.loc[current_row] = value  # type: ignore
    display_queue.put(config.current_dataframe)


def handle_show_df(args, sock: socket.socket, queue: queue.Queue) -> None:
    # assuming that the data was created using the .to_json(orient='split') function
    raise NotImplementedError
    local_logger = logger.getChild("show_df")
    try:
        current_df_sequence = pd.read_json(args, orient="split")
        with lock:
            queue.put(current_df_sequence)
    except Exception as e:
        local_logger.error(f"got exception {e=}")


def handle_get_current_df(*, sock: socket.socket, **kwargs) -> None:
    local_logger = logger.getChild("get_current_df")

    working_df = config.current_dataframe
    local_logger.debug(f"dumping the dataframe to a json string")
    json_text = working_df.to_json(orient="index")  # type: ignore
    json_data = json.dumps(json_text)
    local_logger.debug(f"sending the data")
    send_message(sock, json_data.encode("utf-8"))
    local_logger.debug(f"data sent")


def handle_file(*, value: str, display_queue: queue.Queue, **kwargs):
    # load a csv file
    # load that into a dataframe
    # check that it has the right size
    # check that each element is a hex code
    local_logger = logger.getChild("handle_file")

    if type(value) != str:
        local_logger.error(f"needed a file path, got {type(value)=}, {value=}")
        return
    file_path = Path(value)
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
    display_queue.put(current_df_sequence)


def toggle_fps(**kwargs) -> None:
    config.show_fps = not config.show_fps


def set_stop_event(*, stop_event: threading.Event, **kwargs) -> None:
    stop_event.set()


def handle_verbose_logging(**kwargs) -> None:
    common_send_recv.verbose = not common_send_recv.verbose
    logger.getChild("verbose").debug(
        f"Set the send and recv logging verbose to {common_send_recv.verbose}"
    )


# this is a comment so that I can push an update; THANKS GIT
all_commands = {
    "fps": handle_fps,
    "brightness": handle_brightness,
    "temp": handle_getting_temp,
    "fill": handle_fill,
    "set_one": handle_one,
    "loadfile": handle_file,
    "get_list_of_files": handle_getting_list_of_files,
    "get_log": handle_get_logs,
    "toggle_fps": toggle_fps,
    "stop": set_stop_event,
    "addlist": handle_add_list,
    "get_current_df": handle_get_current_df,
    "verbose": handle_verbose_logging,
}


def error_func(*args, **kwargs):
    local_logger = logger.getChild("error")
    local_logger.error(f"Incorrect Command. {args=} {kwargs=}")


def handle_commands(
    command_queue: queue.Queue,
    display_queue: queue.Queue,
    send_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    local_logger = logger.getChild("dispatcher")
    while not stop_event.is_set():
        try:
            current_request = command_queue.get(timeout=1)
        except queue.Empty:
            continue
        if type(current_request) != dict:
            local_logger.error(
                f"{type(current_request)=} {current_request=} is not of type dict"
            )
            current_request = {"error": "invalid request"}
        local_logger.debug(f"{current_request=}")

        target_command = current_request.get("command", "error")
        target_args = current_request.get("args", None)
        socket = current_request.get("socket", None)

        # cheeky way of doing commands?
        func = all_commands.get(target_command, error_func)
        try:
            func(
                sock=socket,
                value=target_args,
                display_queue=display_queue,
                send_queue=send_queue,
                stop_event=stop_event,
            )
        except:
            pass