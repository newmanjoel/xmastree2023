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

def hex_to_int(hex_color:str):
    """Convert a hex color string to an integer."""
    return int(hex_color[1:], 16)

def int_to_hex(int_color:int):
    """Convert an integer color to a hex string."""
    return f'#{int_color:06X}'

def rgb_to_hex(r:int, g:int, b:int) -> str:
    """Convert RGB values to hex color code."""
    hex_color = f"#{int(r):02X}{int(g):02X}{int(b):02X}"
    return hex_color

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

def create_led_names(led_num:int) -> list[str]:
    return [f'LED_{LED_NUMBER}' for LED_NUMBER in range(led_num)]

def get_formatted_df_from_csv(file_path: Path) -> pd.DataFrame:
    start = time.time()
    df = pd.read_csv(file_path)
    end = time.time()
    logging.getLogger('light_driver').debug(f'took {end-start:.03f}s to load the df from file')
    # column names are [RGB]_[#]
    # FRAME_ID,R_0,G_0,B_0,R_1,G_1,B_1,
    frames = []
    start = time.time()
    for index, row in df.iterrows():
        frames.append(create_array_from_df_row(row))
    end = time.time()
    logging.getLogger('light_driver').debug(f'took {end-start:.03f}s to get the arrays')
    start = time.time()
    led_number = len(frames[0])
    columns = create_led_names(led_number)
    results = pd.DataFrame(frames, columns=columns)
    end = time.time()
    logging.getLogger('light_driver').debug(f'took {end-start:.03f}s to put everything in the df')
    return results

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



def create_array_from_df_row(row: pd.Series) -> list[int]:
    # this took 7.8 seconds while using dicts
    # TODO: if this is slow, this is duplicating a lot of work to get the column headers
    header_values: list[str] = row.axes[0].tolist()
    frame_id = int(row[header_values[0]])
    size_of_series = len(row)
    size_of_array = (size_of_series -1)//3
    logging.getLogger('light_driver').debug(f'{size_of_array=}')

    # result = np.empty((1,(size_of_array-1)//3))
    result = np.array([[0, 0, 0] for _ in range(size_of_array)])

    column_name = "R_12"
  
    for column_name in header_values[1:]:
        # data_value will be [RGB]_[LED #]
        color_str, led_str = column_name.split("_")
        converted_led_number = int(led_str)
        converted_color_value = int(row[column_name])
        working_color:list[int] = result[converted_led_number]
        logging.getLogger('light_driver').debug(f'{converted_led_number=}{converted_color_value=}')
        match color_str:
            case "R":
                working_color[0] = converted_color_value
            case "G":
                working_color[1] = converted_color_value
            case "B":
                working_color[2] = converted_color_value
            case _:
                raise ValueError(
                    f"Got a color of {color_str} for led {led_str}, which is not R,G, or B"
                )
        result[converted_led_number] = working_color
        # logging.getLogger('light_driver').debug(f'{working_color=}')
 
    results = np.empty(size_of_series)
    results[0] = frame_id
    for index, color in enumerate(results):
        logging.getLogger('light_driver').debug(f'{color=}')
        hex_code = rgb_to_hex(int(color[0]),int(color[1]),int(color[2]))
        results[index+1] = hex_to_int(hex_code)
    return results

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

    test_sequence_path = Path(
        r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\examples\running_pi.csv"
    )
    start_time = time.time()
    led_sequence = read_from_csv(test_sequence_path)
    end_time = time.time()
    # print(led_sequence.frames[0])
    print(f"Loading Sequence from CSV took {end_time-start_time:.3f}s to complete")

    df = led_sequence.convert_to_df()
    # print(df)

    df = get_all_info_in_df(led_locations, led_sequence)
    print(df)
