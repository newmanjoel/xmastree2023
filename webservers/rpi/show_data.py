import logging
import re
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


def convert_row_to_ints(
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


def convert_df_to_list_of_int_speedy(input_df: pd.DataFrame) -> list[list[int]]:
    local_logger = logger.getChild("df_2_int")
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

    results = np.apply_along_axis(convert_row_to_ints, 1, raw_data)

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
    return results.tolist()


@log_when_functions_start_and_stop
def cashe_all_ints() -> None:
    import itertools as it

    for color in it.combinations_with_replacement(range(0, 256), 3):
        grb_to_int(color[0], color[1], color[2])


def show_data_on_leds(stop_event: threading.Event, display_queue: queue.Queue) -> None:
    global pixels
    local_logger = logger.getChild("running")
    local_logger.info("Starting")
    # cashing all of the conversion of ints to colors saves a LOT of time
    cashe_all_ints()

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
