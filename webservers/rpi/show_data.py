from functools import lru_cache
import logging
import re
import pandas as pd
import numpy as np
import time
import threading
import queue
import itertools as it
import config
from common.file_parser import grb_to_int
from common.common_objects import (
    all_standard_column_names,
    setup_common_logger,
)

# used for pushing the data out
import board  # type: ignore
import neopixel  # type: ignore

logger = logging.getLogger("display")
logger = setup_common_logger(logger)
column_names = all_standard_column_names(config.led_num)


def setup() -> neopixel.NeoPixel:
    # set up the leds
    pixels = neopixel.NeoPixel(board.D12, config.led_num, auto_write=False)
    pixels.fill((100, 100, 100))
    pixels.show()

    config.pixels = pixels
    return pixels


def sanitize_column_names(input_df: pd.DataFrame) -> pd.DataFrame:
    return_df = input_df.copy(deep=True)

    def is_matching_pattern(s):
        pattern = re.compile(r"^[a-zA-Z]_\d+$")
        return bool(pattern.match(s))

    for name in return_df.columns:
        if not is_matching_pattern(name):
            return_df.drop(name, axis=1, inplace=True)
    return return_df


def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(it.islice(it, n)):
        yield batch


def grb_to_int_fast(color: list[int]) -> int:
    return int((color[1] << 16) | (color[0] << 8) | color[2])


def convert_row_to_ints(input_row: list[int], number_of_columns: int) -> list[int]:
    time1 = time.time()
    return_list = [0] * (number_of_columns // 3)
    time2 = time.time()

    reshaped_data = np.reshape(input_row, (number_of_columns // 3, 3))
    time3 = time.time()
    # logger.getChild("convert_row_to_ints").debug(f"{reshaped_data=}")
    return_list = np.apply_along_axis(grb_to_int_fast, 1, reshaped_data)
    time4 = time.time()
    # logger.getChild("convert_row_to_ints").debug(f"{return_list=}")

    # for pixel_num in range(0, number_of_columns, 3):
    #     led_pixel_index = pixel_num // 3
    #     led_pixel_color = grb_to_int(
    #         input_row[pixel_num], input_row[pixel_num + 1], input_row[pixel_num + 2]
    #     )
    #     return_list[led_pixel_index] = led_pixel_color

    return_list = list(map(int, return_list))
    time5 = time.time()

    logger.getChild("convert_row_to_ints").debug(
        f"{time2-time1:0.6f} {time3-time2:0.6f} {time4-time3:0.6f} {time5-time4:0.6f}"
    )
    # logger.getChild("convert_row_to_ints").debug(f"{return_list=}")

    return return_list


def convert_df_to_list_of_int_speedy(input_df: pd.DataFrame) -> list[list[int]]:
    local_logger = logger.getChild("c_df_2_ints_speedy")
    local_logger.debug("starting conversion")
    start_time = time.time()
    working_df = input_df.copy(deep=True)
    time_2 = time.time()
    working_df = sanitize_column_names(working_df)
    working_df.reindex(column_names, axis=1)
    df_rows, df_columns = working_df.shape
    time_3 = time.time()
    raw_data = working_df.to_numpy(dtype=np.ubyte)
    results = [[0]] * df_rows
    time_4 = time.time()
    led_num = config.led_num
    for row_index, row in enumerate(raw_data):
        row_list = [0] * led_num
        row_list = convert_row_to_ints(row, df_columns)
        results[row_index] = row_list
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

    local_logger.debug(
        f"copy:{copy_time:0.5f} clean:{clean_time:0.5f} types:{unit_change_time:0.5f} looping:{enumerate_time:0.5f} total:{total_time:0.5f}"
    )
    local_logger.debug(
        f"ending conversion, it took {end_time-start_time:0.3f}s to convert the file"
    )
    return results


def show_data_on_leds(stop_event: threading.Event, display_queue: queue.Queue) -> None:
    global pixels
    local_logger = logger.getChild("running")
    local_logger.info("Starting")
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
    local_logger.info("Exiting")


pixels = setup()
