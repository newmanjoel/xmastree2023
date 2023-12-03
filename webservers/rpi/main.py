import functools
import socket
import json
import logging
import colorlog
import queue
import threading

import numpy as np
import pandas as pd
import time
import select
from pathlib import Path

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

fps = 5.0
stop_event = threading.Event()
stop_event.clear()

column_names = [f'LED_{n}' for n in range(150)]

lock = threading.Lock()

# Create DataFrame filling with black
current_sequence = pd.DataFrame("#000000", index=range(1), columns=column_names)

def hex_to_rgb(hex_color:str) -> tuple:
    """Convert GRB hex color code to RGB tuple."""
    hex_color = hex_color.lstrip("#")  # Remove '#' if present
    rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return rgb

def rgb_to_hex(r:int, g:int, b:int) -> str:
    """Convert RGB values to hex color code."""
    hex_color = f"#{r:02X}{g:02X}{b:02X}"
    return hex_color

def handle_fill(args):
    # converts RGB into a GRB hex
    global current_sequence
    if type(args) != list:
        logger.getChild('fill').error(f'trying to fill with something that is not a list {type(args)=}\n{args=}')
        return
    if len(args) != 3:
        logger.getChild('fill').error(f'trying to fill with more than 3 elements {len(args)=}\n{args=}')
        return
    color_r = int(args[0])
    color_g = int(args[1])
    color_b = int(args[2])
    data = (color_g, color_r, color_b)
    color = rgb_to_hex(color_g, color_r, color_b)
    with lock:
        current_sequence = pd.DataFrame(color, index=range(1), columns=column_names)

    logger.getChild('fill').info(f'filling with {color=}')

def handle_fps(args):
    global fps
    try:
        args = float(args)
    except Exception:
        # this is bad, but IDC I check the type later
        pass
    if type(args) == int or type(args)==float:
        fps = args
    else:
        logger.getChild('fps').warning(f'Tried to set the FPS to {args=}, this needs to be a number.')


def handle_file(args):
    global current_sequence
    # load a csv file
    # load that into a dataframe
    # check that it has the right size
    # check that each element is a hex code
    
    if type(args)!=str:
        logger.getChild('file').error(f'needed a file path, got {type(args)=}, {args=}')
        return
    file_path = Path(args)
    if not file_path.exists():
        logger.getChild('file').error(f"File dosn't exist. {file_path=}")
        return

    import file_parser

    start = time.time()
    df = file_parser.get_formatted_df_from_csv(file_path)
    end = time.time()
    logger.getChild('file').debug(f"loaded the file to a dataframe and it took {end-start:0.3f}")
    logger.getChild('file').debug(f"loaded the file to a dataframe and it is using {df.memory_usage(deep=True).sum()}b")
    logger.getChild('file').debug(f"{df}")
    with lock:
        current_sequence = df



def handle_add_list(args):
    global current_sequence, led_num
    if type(args) == list:
        pass
    else:
        logger.getChild('add_list').warning(f'needed a list, but got {type(args)} of {args=}')
        return
    
    if len(args) != led_num:
        logger.getChild('add_list').warning(f'needed a list of len({led_num}), but got {len(args)} of {args=}')
        return
    
    # going to assume this is in order
    # note that the rows and columns are one based and not zero based
    current_row, current_column = current_sequence.shape

    with lock:
        current_sequence.loc[current_row] = args

def handle_command(command:dict, stop_event:threading.Event) -> None:
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
        case "addlist":
            handle_add_list(command['args'])
        case "loadfile":
            handle_file(command['args'])
            pass
        case "fps":
            handle_fps(command['args'])
        case "pause":
            handle_fps(0)
        case "stop":
            stop_event.set()
        case _:
            pass

def log_when_functions_start_and_stop(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.getChild(func.__name__).debug(f"Function {func.__name__} started.")
        result = func(*args, **kwargs)
        logger.getChild(func.__name__).debug(f"Function {func.__name__} ended.")
        return result
    return wrapper

@log_when_functions_start_and_stop
def running(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        for index, row in current_sequence.iterrows():
            for pixel_num in range(led_num):
                pixels[pixel_num] = hex_to_rgb(row[f'LED_{pixel_num}'])
            pixels.show()
            while fps==0:
                time.sleep(0.5)
            time.sleep(1.0/fps)


def handle_received_data(received_data: str, stop_event: threading.Event) -> None:
    try:
        command = json.loads(received_data)
        handle_command(command, stop_event)
    except json.JSONDecodeError as JDE:
        logger.warning(f"{JDE}\n\nInvalid JSON format. Please provide valid JSON data.\n{received_data=}")

@log_when_functions_start_and_stop
def start_server(host:str, port:int, stop_event:threading.Event) -> None:
    local_logger = logger.getChild('webserver')
    try:
        
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setblocking(0)
        server_socket.bind((host, port))
        server_socket.listen(5)

        connected_clients = []

        while not stop_event.is_set():
            readable, _, _ = select.select([server_socket] + connected_clients, [], [], 0.2)
            for sock in readable:
                if sock is server_socket:
                    # New connection, accept it
                    client_socket, client_address = sock.accept()
                    client_socket.setblocking(0)
                    local_logger.info(f"New connection from {client_address}")
                    connected_clients.append(client_socket)
                else:
                    # Data received from an existing client
                    data = sock.recv(10_000)
                    if data:
                        # print(f"Received data: {data.decode()}")
                        handle_received_data(data.decode('utf-8'), stop_event)
                    else:
                        # No data received, the client has closed the connection
                        local_logger.info(f"Connection closed by {sock.getpeername()}")
                        connected_clients.remove(sock)
                        sock.close()
                                    
    except KeyboardInterrupt:
        pass
    finally:
        server_socket.close()


if __name__ == "__main__":
    host = "localhost"  # Change this to the desired host address
    port = 12345           # Change this to the desired port number
    stop_event.clear()

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
            time.sleep(60)
            logger.getChild('main_loop').info('press ctrl+c to stop')
    except KeyboardInterrupt:
        stop_event.set()
        web_server_thread.join()
        running_thread.join()
    logger.info("Application Stopped")

