import logging
import pandas as pd
import numpy as np
import time
import threading
import queue

import config
from common.file_parser import grb_to_int
from common.common_objects import (
    all_standard_column_names,
    log_when_functions_start_and_stop,
)

# used for pushing the data out
import board  # type: ignore
import neopixel  # type: ignore

logger = logging.getLogger("display")
column_names = all_standard_column_names(config.led_num)


def setup() -> neopixel.NeoPixel:
    # set up the leds
    pixels = neopixel.NeoPixel(board.D12, config.led_num, auto_write=False)
    pixels.fill((100, 100, 100))
    pixels.show()

    config.pixels = pixels
    return pixels


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


pixels = setup()
