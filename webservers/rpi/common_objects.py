from dataclasses import dataclass
import functools
from itertools import starmap
from pathlib import Path
import pandas as pd
from pyparsing import col


@dataclass
class Color:
    r: int
    g: int
    b: int

    def enforce_bounds(self) -> None:
        min_value = 0
        max_value = 255

        self.r = int(min(max(self.r, min_value), max_value))
        self.g = int(min(max(self.g, min_value), max_value))
        self.b = int(min(max(self.b, min_value), max_value))

    def to_hex(self) -> str:
        self.enforce_bounds()
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def from_hex(self, hex_string) -> None:
        self.r = int(hex_string[1:3], 16)
        self.g = int(hex_string[3:5], 16)
        self.b = int(hex_string[5:7], 16)
        self.enforce_bounds()


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


@dataclass
class Frame:
    id: int
    lights: list[Led]

    def to_hex_color_dict(self) -> dict[int, str]:
        results = {}
        for led in self.lights:
            results[led.id] = led.color.to_hex()
        return results


@dataclass
class Sequence:
    name: str
    filepath: Path
    frames: list[Frame]

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
        color_dict = self.convert_to_dict()
        results = pd.DataFrame.from_dict(color_dict)
        if include_led_column:
            # TODO: fix this so that it pulls the actual list, and not just assumes
            results["led_id"] = list(range(0, len(self.frames[0].lights)))
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

    color_bounds_check.from_hex("#01FF04")
    print(color_bounds_check)

    # check the sequence to df
