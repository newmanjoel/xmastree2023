import socket
import json
import logging
import colorlog
import queue
import threading

import numpy as np
import pandas as pd
import time

logger = logging.getLogger("light_driver")
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


import board
import neopixel
led_num = 150
pixels = neopixel.NeoPixel(board.D10, led_num, auto_write=False)
pixels.fill((100,100,100))
pixels.show()

fps = 30.0
stop_event = threading.Event()
stop_event.clear()

column_names = [f'LED_{n}' for n in range(150)]

lock = threading.Lock()

# Create DataFrame filling with black
current_sequence = pd.DataFrame("#000000", index=range(1), columns=column_names)

def hex_to_rgb(hex_color:str) -> tuple:
    """Convert hex color code to RGB values."""
    hex_color = hex_color.lstrip("#")  # Remove '#' if present
    rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return rgb

def rgb_to_hex(r:int, g:int, b:int) -> str:
    """Convert RGB values to hex color code."""
    hex_color = f"#{r:02X}{g:02X}{b:02X}"
    return hex_color

def handle_fill(args):
    global current_sequence
    color_r = int(args[0])
    color_g = int(args[1])
    color_b = int(args[2])
    data = (color_g, color_r, color_b)
    color = rgb_to_hex(color_r, color_g, color_b)
    with lock:
        current_sequence = pd.DataFrame(color, index=range(1), columns=column_names)

    logger.getChild('fill').info(f'filling with {color=}')



def handle_command(command:dict, stop_event:threading.Event) -> None:
    global fps
    # Define the logic to handle different commands
    logger.debug(f"{command=}")
    target_command = command["command"]
    match target_command:
        case "fill":
            handle_fill(command["args"])
        case "off":
            handle_fill([0,0,0])
        case "single":
            # handle_one(command['args'])
            pass
        case "list":
            # handle_list(command['args'])
            pass
        case "file":
            # handle_file(command['args'])
            pass
        case "pause":
            fps = 0
        case "stop":
            stop_event.set()
        case _:
            pass
    

def running(stop_event: threading.Event):
    while not stop_event:
        for index, row in current_sequence.iterrows():
            for pixel_num in range(led_num):
                pixels[pixel_num] = hex_to_rgb(row[f'LED_{pixel_num}'])
            pixels.show()
            while fps==0:
                time.sleep(0.5)
            time.sleep(1/fps)


def start_server(host:str, port:int, stop_event:threading.Event):
    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            try:
                if stop_event:
                    raise KeyboardInterrupt
                server_socket.bind((host, port))
                server_socket.listen()

                logger.info(f"Server listening on {host}:{port}")

                while not stop_event:
                    client_socket, client_address = server_socket.accept()
                    with client_socket:
                        logger.debug(f"Connection established from {client_address}")

                        data = client_socket.recv(1024)
                        if not data:
                            break
                        try:
                            decoded_data = data.decode('utf-8').strip()
                            # logger.debug(f'{decoded_data=}')
                            command = json.loads(decoded_data)
                            handle_command(command, stop_event)
                        except json.JSONDecodeError as JDE:
                            logger.warning(f"{JDE}\n\nInvalid JSON format. Please provide valid JSON data.\n{data.decode('utf-8').strip()}")
                    logger.debug(f'Connection ended from {client_address}')
            except KeyboardInterrupt:
                pass
            finally:
                server_socket.close()

if __name__ == "__main__":
    host = "localhost"  # Change this to the desired host address
    port = 12345           # Change this to the desired port number

    # start_server(host, port)

    # Create a producer thread
    web_server_thread = threading.Thread(target=start_server, args=(host, port,stop_event))

    # Create a consumer thread
    running_thread = threading.Thread(target=running, args=(stop_event,))

    # Start the threads
    web_server_thread.start()
    running_thread.start()

    try:
        while True:
            time.sleep(1)
            logger.getChild('main_loop').info('press ctrl+c to stop')
    except KeyboardInterrupt:
        stop_event.set()
        web_server_thread.join()
        running_thread.join()

