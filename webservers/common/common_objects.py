from dataclasses import dataclass
from typing import NamedTuple
import functools
from pathlib import Path
import pandas as pd
import logging
from functools import lru_cache
import colorlog
import time


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


class Color(NamedTuple):
    r: int
    g: int
    b: int

    def to_hex(self) -> str:
        # self.enforce_bounds()
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"


def color_from_hex(hex_string: str) -> Color:
    return Color(
        int(hex_string[1:3], 16), int(hex_string[3:5], 16), int(hex_string[5:7], 16)
    )


@dataclass
class Led_Location:
    led_id: int
    x: float
    y: float
    z: float

    def to_dict(self) -> dict[str, float]:
        return {"x": self.x, "y": self.y, "z": self.z}

    def to_array(self) -> list[float]:
        return [self.x, self.y, self.z]


@dataclass
class Led:
    id: int
    color: Color


@lru_cache(maxsize=2)
def create_led_names(led_num: int) -> list[str]:
    return [f"LED_{LED_NUMBER}" for LED_NUMBER in range(led_num)]


@lru_cache(maxsize=2)
def all_standard_column_names(num: int) -> list[str]:
    results = []
    for i in range(num):
        results.append(f"R_{i}")
        results.append(f"G_{i}")
        results.append(f"B_{i}")
    return results


@dataclass
class Frame:
    id: int
    lights: list[Led]

    def as_array(self) -> list[str]:
        return list(map(Color.to_hex, [x.color for x in self.lights]))

    def to_hex_color_dict(self) -> dict[int, str]:
        results = {}
        for led in self.lights:
            results[led.id] = led.color.to_hex()
        return results

    def create_from_series(
        self, input_series: pd.Series, frame_id: int, hex_colors: bool = True
    ) -> None:
        # clear current values
        self.lights = []
        self.id = frame_id

        for index, value in input_series.items():
            hex_color = Color(0, 0, 0)
            hex_color.from_hex(value)
            led = Led(index, hex_color)
            self.lights.append(led)

    def convert_to_df(self) -> pd.DataFrame:
        led_num = len(self.lights)
        columns = create_led_names(led_num)
        led_colors = [x.color for x in self.lights]
        data = [x.to_hex() for x in led_colors]

        df = pd.DataFrame([], columns=columns)
        df.loc[0] = data
        return df

    def convert_to_RGB_df(self) -> pd.DataFrame:
        rgb = ["R", "G", "B"]
        columns = ["FRAME_ID"]
        data = [self.id]
        for light in self.lights:
            for color in rgb:
                columns.append(f"{color}_{light.id}")
                if color == "R":
                    data.append(light.color.r)
                elif color == "G":
                    data.append(light.color.g)
                elif color == "B":
                    data.append(light.color.b)

        # data = list(map(Color.to_hex, led_colors))
        # logging.getLogger('light_driver').debug(f'{led_num=}\n{columns=}\n{data=}')
        df = pd.DataFrame([], columns=columns)
        df.loc[0] = data
        return df


@dataclass
class Sequence:
    name: str
    filepath: Path
    frames: list[Frame]

    def create_from_df(self, input_df: pd.DataFrame, name: str, filepath: Path):
        index: int = 0
        row: pd.Series = None  # type: ignore
        self.frames = []
        self.filepath = filepath
        self.name = name

        for index, row in input_df.iterrows():  # type: ignore
            u_frame = Frame(0, [])
            u_frame.create_from_series(row, index)
            self.frames.append(u_frame)

    def convert_to_dict(self) -> dict[int, dict[int, str]]:
        results = {}
        for frame in self.frames:
            results[frame.id] = frame.to_hex_color_dict()
        return results

    def convert_to_flat_df(self) -> pd.DataFrame:
        # Frame_ID, LED_ID, Color
        df = self.convert_to_df()
        return df.melt(id_vars=["led_id"], var_name="frame_id", value_name="led_color")

    def convert_to_df(self, include_led_column: bool = True) -> pd.DataFrame:
        start = time.time()
        list_of_dfs = list(map(Frame.convert_to_df, self.frames))
        end = time.time()
        logging.getLogger("light_driver").debug(
            f"took {end-start:.03f}s to convert to dfs"
        )
        start = time.time()
        results = pd.concat(list_of_dfs, ignore_index=True)
        end = time.time()
        logging.getLogger("light_driver").debug(
            f"took {end-start:.03f}s to concat the dfs"
        )
        return results


class SequenceWithLocation(Sequence):
    location: list[Led_Location]


def convert_list_of_coords_to_locations(
    input_list: list[list[int]],
) -> list[Led_Location]:
    results = []
    for index, item in enumerate(input_list):
        # item will be [x,y,z]
        temp_item = Led_Location(led_id=index, x=item[0], y=item[1], z=item[2])
        results.append(temp_item)
    return results


def get_xyz_from_locations(
    input_list: list[Led_Location],
) -> tuple[list[float], list[float], list[float]]:
    x = []
    y = []
    z = []
    for location in input_list:
        x.append(location.x)
        y.append(location.y)
        z.append(location.z)
    return (x, y, z)


def get_locations_as_dict(loc: list[Led_Location]) -> dict[int, dict[str, float]]:
    results = {}
    for location in loc:
        results[location.led_id] = location.to_dict()
    return results


def get_locations_as_array(loc: list[Led_Location]) -> list[float]:
    results = []
    for location in loc:
        temp = [location.led_id]
        temp.extend(location.to_array())  # type: ignore
        results.append(temp)
    return results


def get_all_info_in_df(loc: list[Led_Location], seq: Sequence) -> pd.DataFrame:
    loc_arr = get_locations_as_array(loc)
    loc_arr.sort()
    results = seq.convert_to_df()

    # not doing any checking if the id is the same ¯\_(ツ)_/¯
    results["x"] = [arr[1] for arr in loc_arr]  # type: ignore
    results["y"] = [arr[2] for arr in loc_arr]  # type: ignore
    results["z"] = [arr[3] for arr in loc_arr]  # type: ignore

    return results


def get_value(led_id: int, location_dict: dict, axis: str):
    local_location = location_dict[led_id]
    return local_location[axis]


def all_info_for_plotting(loc: list[Led_Location], seq: Sequence) -> pd.DataFrame:
    loc_dict = get_locations_as_dict(loc)
    results = seq.convert_to_flat_df()
    get_x = functools.partial(get_value, location_dict=loc_dict, axis="x")
    get_y = functools.partial(get_value, location_dict=loc_dict, axis="y")
    get_z = functools.partial(get_value, location_dict=loc_dict, axis="z")
    results["x"] = list(map(get_x, results["led_id"]))
    results["y"] = list(map(get_y, results["led_id"]))
    results["z"] = list(map(get_z, results["led_id"]))

    return results


if __name__ == "__main__":
    color_bounds_check = Color(-1, 2555, 4)
    print(color_bounds_check.to_hex())

    print(color_bounds_check)

    # check the sequence to df
