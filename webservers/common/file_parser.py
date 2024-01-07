# import operator
from pathlib import Path

import pandas as pd

try:
    from common_objects import (
        Color,
        Led,
        Frame,
        Led_Location,
        Sequence,
        get_all_info_in_df,
    )
except ImportError:
    import sys, os

    sys.path.append(os.path.dirname(__file__))
    from common_objects import (
        Color,
        Led,
        Frame,
        Led_Location,
        Sequence,
        get_all_info_in_df,
    )
import logging
import time
import numpy as np

logger = logging.getLogger("file_parser")


def hex_to_int(hex_color: str):
    """Convert a hex color string to an integer."""
    return int(hex_color[1:], 16)


def int_to_hex(int_color: int):
    """Convert an integer color to a hex string."""
    return f"#{int_color:06X}"


def rgb_to_hex(r: int, g: int, b: int) -> str:
    """Convert RGB values to hex color code."""
    hex_color = f"#{int(r):02X}{int(g):02X}{int(b):02X}"
    return hex_color


def rgb_to_int(r: int, g: int, b: int) -> int:
    return int((r << 16) | (g << 8) | b)


def grb_to_int(g: int, r: int, b: int) -> int:
    return int((r << 16) | (g << 8) | b)


def read_GIFT_file(file_path: Path) -> tuple[list[Led_Location], pd.DataFrame]:
    df = pd.read_csv(file_path, names=["x", "y", "z"])

    leds = []
    for index, row in df.iterrows():
        temp_row = Led_Location(index, row.iloc[0], row.iloc[1], row.iloc[2])  # type: ignore
        leds.append(temp_row)
    return (leds, df)


def save_GIFT_file(lights: list[Led_Location], file_path: Path) -> str:
    # sorted_led_list = lights.sort(key=operator.attrgetter("led_id"))
    file_path.touch(exist_ok=True)
    csv_content = ""
    for loc in lights:
        csv_content += f"{loc.x:.10f},{loc.y:.10f},{loc.z:.10f}\n"
    file_path.write_text(csv_content)
    return f"{file_path.absolute()}"


def create_led_names(led_num: int) -> list[str]:
    return [f"LED_{LED_NUMBER}" for LED_NUMBER in range(led_num)]


def read_from_csv(file_path: Path) -> Sequence:
    df = pd.read_csv(file_path)
    # column names are [RGB]_[#]
    # FRAME_ID,R_0,G_0,B_0,R_1,G_1,B_1,
    frames = []
    for index, row in df.iterrows():
        # print(row.__dict__)
        temp_row = create_frame_from_df_row(row)
        frames.append(temp_row)

    return Sequence(name=file_path.name, filepath=file_path, frames=frames)


def create_frame_from_df_row(row: pd.Series) -> Frame:
    # TODO: if this is slow, this is duplicating a lot of work to get the column headers
    header_values: list[str] = row.axes[0].tolist()
    frame_id = int(row[header_values[0]])
    column_name = "R_12"
    led_dict = {}
    for column_name in header_values[1:]:
        # data_value will be [RGB]_[LED #]
        color_str, led_str = column_name.split("_")
        converted_led_number = int(led_str)
        converted_color_value = float(row[column_name])
        working_color = led_dict.get(converted_led_number, Color(0, 0, 0))
        match color_str:
            case "R":
                working_color.r = converted_color_value
            case "G":
                working_color.g = converted_color_value
            case "B":
                working_color.b = converted_color_value
            case _:
                raise ValueError(
                    f"Got a color of {color_str} for led {led_str}, which is not R,G, or B"
                )
        led_dict[converted_led_number] = working_color

    leds = []
    for key, value in led_dict.items():
        leds.append(Led(id=key, color=value))

    leds.sort(key=lambda x: x.id)
    # print(color_str, led_str)
    return Frame(id=frame_id, lights=leds)


if __name__ == "__main__":
    import time
    import colorlog

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    color_formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
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

    test_GIFT_file = False
    test_CSV_file = True
    testing_on_rpi = False

    if test_GIFT_file:
        test_GIFT_path = Path(
            r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\coords_2021.csv"
        )
        test_save_GIFT_path = Path(
            r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\coords_2023_test_save.gift"
        )
        start_time = time.time()
        led_locations, df = read_GIFT_file(test_GIFT_path)
        end_time = time.time()
        # print(led_locations[0])
        print(f"Loading GIFT File took {end_time-start_time:.3f}s to complete")

    # save_GIFT_file(led_locations, test_save_GIFT_path)

    if test_CSV_file:
        if testing_on_rpi:
            test_sequence_path = Path(
                r"/home/pi/github/xmastree2023/examples/pulsing_heart.csv"
            )
        else:
            test_sequence_path = Path(
                r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\examples\pulsing_heart.csv"
            )
        start_time = time.time()
        led_sequence = read_from_csv(test_sequence_path)
        end_time = time.time()
        # print(led_sequence.frames[0])
        print(f"Loading Sequence from CSV took {end_time-start_time:.3f}s to complete")

        start_time = time.time()
        df = led_sequence.convert_to_df()
        end_time = time.time()
        print(f"converting to a dataframe took {end_time-start_time:.3f}s to complete")
        # print(df)

        # df = get_all_info_in_df(led_locations, led_sequence)
        print(df)
