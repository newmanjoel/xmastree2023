import enum
import json
from pathlib import Path
import socket
import pandas as pd
import pynput
from pynput import keyboard
from dataclasses import dataclass
import csv

amount = 0.1
rpi_port, rpi_ip = (12345, "192.168.1.192")


class direction_enum(enum.Enum):
    UP = enum.auto()
    DOWN = enum.auto()
    LEFT = enum.auto()
    RIGHT = enum.auto()
    IN = enum.auto()
    OUT = enum.auto()


class plane_axis(enum.Enum):
    X = enum.auto()
    Y = enum.auto()
    Z = enum.auto()


@dataclass
class Moveable:
    x: float
    y: float
    z: float


@dataclass
class Plane(Moveable):
    axis: plane_axis
    tolerance: float

    def get_all_points_near_plane(self, point_locations: dict) -> list[int]:
        results = []
        if self.axis == plane_axis.X:
            for key, value in point_locations.items():
                if (value["x"] - self.x) <= self.tolerance:
                    results.append(key)
        return results


@dataclass
class Point(Moveable):
    index: int

    def from_tuple(self, input_values: tuple) -> None:
        self.x = input_values[0]
        self.y = input_values[1]
        self.z = input_values[2]


working_point = Point(x=0, y=0, z=0, index=0)
working_plane = Plane(x=0, y=0, z=0, axis=plane_axis.X, tolerance=0.3)
all_points: list[Point] = []
gift_file_path = Path(
    r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\coords_2023_test_save.gift"
)


def load_csv_to_dict(file_path: Path) -> dict[int, tuple[float, float, float]]:
    global all_points
    data_dict = {}
    with open(file_path, "r") as csvfile:
        csv_reader = csv.reader(csvfile)

        for row_number, row in enumerate(csv_reader):
            try:
                # Assuming the format x,y,z in each row
                x, y, z = map(float, row)
                key = int
                data_dict[row_number] = (x, y, z)
                all_points.append(Point(x, y, z, row_number))
            except ValueError:
                # Handle invalid rows with incorrect number of values
                print(f"Skipping invalid row: {row}")
    return data_dict


dict_of_points = load_csv_to_dict(gift_file_path)
light_num = len(all_points)


def all_standard_column_names(num: int) -> list[str]:
    results = []
    for i in range(num):
        results.append(f"R_{i}")
        results.append(f"G_{i}")
        results.append(f"B_{i}")
    return results


column_names = all_standard_column_names(light_num)


def update_webserver_to_show_point(point: Point, plane: Plane):
    # set all points on the plane to be red
    # set the specific point to be green
    # set all other points to be black

    # create the dataframe with all black
    current_df_sequence = pd.DataFrame(0, index=range(1), columns=column_names)
    current_df_sequence[f"R_{point.index}"] = 255

    df_data = current_df_sequence.to_dict(orient="split")

    data = {"command": "show_df", "args": df_data}
    json_data = json.dumps(data)
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        connection_to_rpi.sendall(json_data.encode("utf-8"))


def move_thing(
    direction: direction_enum, thing_to_move: Moveable, amount: float = 0.1
) -> None:
    print(f"trying to move {thing_to_move} in the direction of {direction} by {amount}")
    match direction:
        case direction_enum.UP:
            # positive z
            thing_to_move.z += amount
            pass
        case direction_enum.DOWN:
            # negative z
            thing_to_move.z -= amount
            pass
        case direction_enum.LEFT:
            # negagive x
            thing_to_move.x -= amount
            pass
        case direction_enum.RIGHT:
            # positive x
            thing_to_move.x += amount
            pass
        case direction_enum.IN:
            # positive y
            thing_to_move.y += amount
            pass
        case direction_enum.OUT:
            # negative y
            thing_to_move.y -= amount
            pass


def on_press(key) -> bool:
    global amount, working_point
    print(f"=> {key}, {type(key)=} {str(key)=}")
    match str(key):
        case "Key.page_up":
            amount += 0.1
            print(f"{amount=}")
        case "Key.page_down":
            amount -= 0.1
            amount = max([amount, 0])  # make sure it never goes negative
            print(f"{amount=}")
        case "Key.up":
            move_thing(direction_enum.UP, working_point, amount)
        case "Key.down":
            move_thing(direction_enum.DOWN, working_point, amount)
        case "Key.left":
            move_thing(direction_enum.LEFT, working_point, amount)
        case "Key.right":
            move_thing(direction_enum.RIGHT, working_point, amount)
        case "'['":
            working_point = all_points[(working_point.index - 1) % light_num]
        case "']'":
            working_point = all_points[(working_point.index + 1) % light_num]
        case "Key.esc":
            return False
        case _:
            pass
    print(f"{working_point=}")
    update_webserver_to_show_point(working_point, working_plane)
    return True


def on_release(key):
    print("=< %s" % key)


with keyboard.Listener(on_press=on_press, on_release=on_release) as listener:
    print("Press esc to exit")
    listener.join()
