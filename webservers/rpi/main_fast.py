from io import StringIO
import socket
import threading

import pandas as pd
from requests import JSONDecodeError
import uvicorn
import logging
import colorlog
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi import Request

logger = logging.getLogger("christmas_lights_web")
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


class JsonData(BaseModel):
    json_raw: str


app = FastAPI()


@app.post("/alloff")
def alloff():
    """Turn off all of the lights"""
    logger.getChild("all_off").info(f"turning off all the lights")


@app.get("/temp")
def get_rpi_temp():
    """measure the temperature of the raspberry pi"""
    import subprocess

    result = subprocess.run(
        ["vcgencmd", "measure_temp"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return result.stdout


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
    uvicorn.run(app, host=host, port=port)
