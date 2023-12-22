import socket

import pandas as pd
from requests import JSONDecodeError
import uvicorn
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi import Request
import json
from pathlib import Path
import random


import os
import sys

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)

from common.common_send_recv import send_message, receive_message
from common.common_objects import setup_common_logger

logger = logging.getLogger("christmas_lights_web")
logger = setup_common_logger(logger)

# rpi_ip = "localhost"
rpi_ip = "192.168.4.205"
rpi_port = 12345


class JsonData(BaseModel):
    json_raw: str


app = FastAPI()


def rgb_to_hex(r: int, g: int, b: int) -> str:
    """Convert RGB values to hex color code."""
    hex_color = f"#{r:02X}{g:02X}{b:02X}"
    return hex_color


def send_dict_to_rpi(message: dict) -> None:
    json_data = json.dumps(message)
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, json_data.encode("utf-8"))


def send_one_message_to_rpi(message: bytes) -> None:
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, message)


def send_and_receive_one_message_to_rpi(message: bytes) -> bytes:
    received_message = b""
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, message)
        received_message = receive_message(connection_to_rpi)
    return received_message


@app.get("/get_logs")
def get_logs():
    data = {"command": "get_log", "args": ""}
    json_data = json.dumps(data)
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, json_data.encode("utf-8"))
        json_bytes = receive_message(connection_to_rpi)
        json_text = json.loads(json_bytes.decode("utf-8"))
    return json_text


@app.post("/alloff")
def alloff():
    """Turn off all of the lights"""
    data = {"command": "fill", "args": [0, 0, 0]}
    json_data = json.dumps(data)
    send_dict_to_rpi(json_data)
    logger.getChild("all_off").info(f"turning off all the lights")


@app.post("/allRGB")
def allred(r: int, g: int, b: int):
    """Turn on the RGB lights"""
    data = {"command": "fill", "args": [r, g, b]}
    json_data = json.dumps(data)
    send_dict_to_rpi(json_data)
    logger.getChild("all_red").info(f"turning off all the lights")


@app.post("/oneoff")
def oneoff(index: int):
    """turn off one light at the given index"""
    logger.getChild("one_off").info(f"turn off the light at {index=}")


@app.post("/speed")
def set_speed(fps: float):
    """set the desired FPS that the sequence will run at. Note that there is an upper limit to this."""
    data = {"command": "fps", "args": fps}
    send_dict_to_rpi(data)
    logger.getChild("speed").info(f"setting the {fps=}")


@app.post("/addRandomColor")
def addRandomColor():
    """add a random color to the existing sequence"""
    # TODO: THIS IS BROKEN
    n = 150
    list_to_add = []
    random_color = (
        random.randint(0, 255),
        random.randint(0, 255),
        random.randint(0, 255),
    )

    for i in range(n):
        list_to_add.append(random_color)
    data = {"command": "addlist", "args": list_to_add}
    # json_data = json.dumps(data)
    send_dict_to_rpi(data)
    logger.getChild("addRandomColor").info(
        f"added the color {random_color} to the current sequence"
    )


@app.get("/temp")
def get_rpi_temp():
    """measure the temperature of the raspberry pi"""
    data = {"command": "temp", "args": ""}
    json_data = json.dumps(data)
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, json_data.encode("utf-8"))
        json_bytes = receive_message(connection_to_rpi)
        json_text = json.loads(json_bytes.decode("utf-8"))
    return json_text


@app.post("/brightness")
def set_light_brightness(brightness: float):
    """Set the brightness precentage. Valid numbers between 1 and 100"""
    data = {"command": "brightness", "args": brightness}
    json_data = json.dumps(data)
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, json_data.encode("utf-8"))


@app.post("/loadfile")
def load_csv_file_on_rpi(file_path: str):
    """Tell the controller what file you want it to load"""
    data = {"command": "loadfile", "args": file_path}
    json_data = json.dumps(data)
    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, json_data.encode("utf-8"))

    return None


@app.get("/files")
def get_list_of_csvs():
    """Return a list of the current CSV's that can be played"""
    data = {"command": "get_list_of_files", "args": ""}
    json_data = json.dumps(data)

    with socket.create_connection((rpi_ip, rpi_port)) as connection_to_rpi:
        send_message(connection_to_rpi, json_data.encode("utf-8"))
        json_bytes = receive_message(connection_to_rpi)
        json_text = json.loads(json_bytes.decode("utf-8"))

    return json_text


@app.post("/receivedf")
async def receive_dataframe(request: Request):
    """
    Create an item.

    :param item: The data to create the item.
    :return: The created item.
    """

    content_type = request.headers.get("Content-Type")

    if content_type is None:
        return "No Content-Type provided."
    elif content_type == "application/dict":
        bstring = await request.body()
        data = pd.read_json(bstring.decode())
        logger.getChild("receive_dataframe").debug(f"\n{data}")
        return 200
    elif content_type == "application/json":
        try:
            json = await request.json()
            df = pd.read_json(json)
            logger.getChild("receive_dataframe").debug(
                f"received_dataframe: \n{json}\n{df}"
            )
            return json
        except JSONDecodeError:
            return "Invalid JSON data."
    else:
        return "Content-Type not supported."


# def handle_connection(client_socket, client_address):
#     # TODO: Implement your connection handling logic here
#     # You can read from or write to the connection socket
#     local_logger = logger.getChild("handle_connection")

#     command = client_socket.recv(1024).decode().strip()
#     local_logger.debug(f"Received command: {command}")

#     if command == "alloff":
#         alloff()
#     elif command == "senddf":
#         receive_dataframe(client_socket)
#     else:
#         local_logger.warning(f"default command case with command {command}")

#     # Close the connection socket when done
#     client_socket.close()
#     local_logger.debug(f"Closed connection to {client_address}")


if __name__ == "__main__":
    host = "localhost"
    port = 1234
    logger = logging.getLogger("main_fast")
    logger = setup_common_logger(logger)
    uvicorn.run(app, host=host, port=port)
