# generic helpful things


import logging
import numpy as np
import pandas as pd
import time


# used for multi-threading
import threading
import queue
from io import StringIO

# used for pushing the data out
import board  # type: ignore
import neopixel  # type: ignore

# used for being able to import stuff from other folders
import os
import sys

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)


# used for sharing data between modules
import config

import common.common_send_recv as common_send_recv
from common.common_send_recv import receive_message
from common.common_objects import setup_common_logger, log_when_functions_start_and_stop
from common.file_parser import grb_to_int
from handle_web_commands import handle_commands, column_names, handle_networking


logger = logging.getLogger("light_driver")
logger = setup_common_logger(logger)

# set up the capture on the root logger so it capture everything
log_capture = StringIO()
logging.getLogger().addHandler(logging.StreamHandler(log_capture))

config.log_capture = log_capture


# set up the leds
pixels = neopixel.NeoPixel(board.D12, config.led_num, auto_write=False)
pixels.fill((100, 100, 100))
pixels.show()

config.pixels = pixels

# set up the queues and events
stop_event = threading.Event()
stop_event.clear()
display_queue = queue.Queue()
shared_web_command_queue = queue.Queue()

# put something in the command_queue right away so that it can boot to something
shared_web_command_queue.put(
    {
        "command": "loadfile",
        "args": "/home/pi/github/xmastree2023/examples/rainbow-implosion.csv",
    }
)


def convert_df_to_list_of_int_speedy(input_df: pd.DataFrame) -> list[list[int]]:
    local_logger = logger.getChild("c_df_2_ints_speedy")
    local_logger.debug("starting conversion")
    start_time = time.time()
    working_df = input_df.copy(deep=True)
    if "FRAME_ID" in working_df.columns:
        working_df = working_df.drop("FRAME_ID", axis=1)
    working_df.reindex(column_names, axis=1)
    df_rows, df_columns = working_df.shape
    raw_data = working_df.to_numpy(dtype=np.ubyte)
    results = [[0]] * df_rows
    for row_index, row in enumerate(raw_data):
        row_list = [0] * config.led_num
        for pixel_num in range(0, df_columns, 3):
            row_list[pixel_num // 3] = grb_to_int(
                row[pixel_num], row[pixel_num + 1], row[pixel_num + 2]
            )
        results[row_index] = row_list
    end_time = time.time()
    local_logger.debug(
        f"ending conversion, it took {end_time-start_time:0.3f}s to convert the file"
    )
    return results


@log_when_functions_start_and_stop
def show_data_on_leds(stop_event: threading.Event, display_queue: queue.Queue) -> None:
    global pixels
    local_logger = logger.getChild("running")
    data = [100, 0, 0] * config.led_num
    working_df = pd.DataFrame([data], index=range(1), columns=column_names)
    fast_array = convert_df_to_list_of_int_speedy(working_df)
    led_amount = int(config.led_num)
    while not stop_event.is_set():
        if not display_queue.empty():
            try:
                working_df: pd.DataFrame = display_queue.get()
                config.current_dataframe = working_df
                local_logger.info("Changing to new df")
                fast_array = convert_df_to_list_of_int_speedy(working_df)
            except queue.Empty as e:
                pass

        for row in fast_array:
            if stop_event.is_set() or not display_queue.empty():
                break
            time1 = time.time()
            pixels[0:led_amount] = row
            time2 = time.time()
            pixels.show()
            time3 = time.time()
            loop_time = time3 - time1
            fps_time = 1.0 / config.fps
            sleep_time = fps_time - loop_time
            if sleep_time < 0:
                sleep_time = 0
            while config.fps == 0:
                time.sleep(0.5)
            else:
                time.sleep(sleep_time)
            time4 = time.time()
            # Loading Array:0.034s Pushing Pixels:0.018s sleeping:0.000s actual_FPS:19.146
            # Loading Array:0.007s Pushing Pixels:0.019s sleeping:0.000s actual_FPS:38.318
            if config.show_fps:
                packing_the_pixels = time2 - time1
                pushing_the_pixels = time3 - time2
                sleeping_time = time4 - time3
                total_time = time4 - time1
                total_fps = 1 / total_time
                local_logger.debug(
                    f"Loading Array:{packing_the_pixels:.3f}s Pushing Pixels:{pushing_the_pixels:.3f}s sleeping:{sleeping_time:.3f}s actual_FPS:{total_fps:.3f}"
                )


if __name__ == "__main__":
    stop_event.clear()

    web_server_thread = threading.Thread(
        target=handle_networking,
        args=(config.host, config.rx_port, stop_event, shared_web_command_queue),
    )

    command_thread = threading.Thread(
        target=handle_commands,
        args=(shared_web_command_queue, display_queue, stop_event),
    )

    running_thread = threading.Thread(
        target=show_data_on_leds, args=(stop_event, display_queue)
    )

    # Start the threads
    web_server_thread.start()
    command_thread.start()
    running_thread.start()

    try:
        while stop_event.is_set():
            time.sleep(120)
            logger.getChild("main_loop").info("press ctrl+c to stop")
            config.log_capture.truncate(100_000)
    except KeyboardInterrupt:
        stop_event.set()
        web_server_thread.join()
        command_thread.join()
        running_thread.join()
    logger.info("Application Stopped")
