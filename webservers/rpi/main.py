import functools

import socket
import json
import logging
import threading
import queue

import board
import neopixel


import pandas as pd
import time
import select
from pathlib import Path


import os
import sys

import config

from handle_web_commands import handle_commands


# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)

from common.common_send_recv import receive_message
from common.common_objects import setup_common_logger
from common.file_parser import rgb_to_int
from handle_web_commands import handle_commands, column_names


logger = logging.getLogger("light_driver")
logger = setup_common_logger(logger)


# set up global variables that will be shared accros the threads

pixels = neopixel.NeoPixel(board.D12, int(config.led_num), auto_write=False)
pixels.fill((100, 100, 100))
pixels.brightness = config.brightness
pixels.show()


stop_event = threading.Event()
stop_event.clear()

display_queue = queue.Queue()
shared_web_command_queue = queue.Queue()


lock = threading.Lock()


# shared_queue.put(current_df_sequence)


def handle_add_list(args, queue: queue.Queue) -> None:
    raise NotImplementedError
    if type(args) == list:
        pass
    else:
        logger.getChild("add_list").warning(
            f"needed a list, but got {type(args)} of {args=}"
        )
        return

    if len(args) != config.led_num:
        logger.getChild("add_list").warning(
            f"needed a list of len({config.led_num}), but got {len(args)} of {args=}"
        )
        return

    # going to assume this is in order
    # note that the rows and columns are one based and not zero based
    current_row, current_column = current_df_sequence.shape

    with lock:
        current_df_sequence.loc[current_row] = args
        queue.put(current_df_sequence)


def handle_show_df(args, sock: socket.socket, queue: queue.Queue) -> None:
    # assuming that the data was created using the .to_json(orient='split') function
    local_logger = logger.getChild("show_df")
    try:
        current_df_sequence = pd.read_json(args, orient="split")
        with lock:
            queue.put(current_df_sequence)
    except Exception as e:
        local_logger.error(f"got exception {e=}")


def log_when_functions_start_and_stop(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.getChild(func.__name__).debug(f"Function {func.__name__} started.")
        result = func(*args, **kwargs)
        logger.getChild(func.__name__).debug(f"Function {func.__name__} ended.")
        return result

    return wrapper


def convert_df_to_list_of_tuples(input_df: pd.DataFrame) -> list[list[tuple]]:
    local_logger = logger.getChild("c_df_2_l")
    local_logger.debug("starting conversion")
    df_rows, df_columns = input_df.shape
    results = [None] * df_rows
    for index, row in input_df.iterrows():
        row_list = [None] * config.led_num

        for pixel_num in range(config.led_num):
            row_list[pixel_num] = (  # type: ignore
                row[f"G_{pixel_num}"],
                row[f"R_{pixel_num}"],
                row[f"B_{pixel_num}"],
            )

        results[index] = row_list  # type: ignore
    local_logger.debug("ending conversion")
    # local_logger.debug(f"\n{results}")
    return results  # type: ignore


def convert_df_to_list_of_ints(input_df: pd.DataFrame) -> list[list[tuple]]:
    local_logger = logger.getChild("c_df_2_ints")
    local_logger.debug("starting conversion")
    df_rows, df_columns = input_df.shape
    results = [None] * df_rows
    for index, row in input_df.iterrows():
        row_list = [None] * config.led_num

        for pixel_num in range(config.led_num):
            row_list[pixel_num] = rgb_to_int(  # type: ignore
                row[f"R_{pixel_num}"], row[f"G_{pixel_num}"], row[f"B_{pixel_num}"]
            )

        results[index] = row_list  # type: ignore
    local_logger.debug("ending conversion")
    # local_logger.debug(f"\n{results}")
    return results  # type: ignore


@log_when_functions_start_and_stop
def running_with_standard_file(
    stop_event: threading.Event, display_queue: queue.Queue
) -> None:
    global pixels
    local_logger = logger.getChild("running")
    # data = [0, 0, 0] * config.led_num

    working_df = pd.DataFrame(
        [[0, 0, 0] * config.led_num], index=range(1), columns=column_names
    )
    fast_array = convert_df_to_list_of_ints(working_df)
    led_amount = int(config.led_num)
    while not stop_event.is_set():
        if not display_queue.empty():
            try:
                working_df = display_queue.get()
                local_logger.info("Changing to new df")
                fast_array = convert_df_to_list_of_tuples(working_df)
            except queue.Empty as e:
                pass

        for row in fast_array:
            if stop_event.is_set() or not display_queue.empty():
                break
            time1 = time.time()
            for led_number in range(led_amount):
                pixels[led_number] = row[led_number]
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
            # lets get that loading array down
            if config.show_fps:
                packing_the_pixels = time2 - time1
                pushing_the_pixels = time3 - time2
                sleeping_time = time4 - time3
                total_time = time4 - time1
                total_fps = 1 / total_time
                local_logger.debug(
                    f"Loading Array:{packing_the_pixels:.3f}s Pushing Pixels:{pushing_the_pixels:.3f}s sleeping:{sleeping_time:.3f}s actual_FPS:{total_fps:.3f}"
                )


def handle_received_data(
    received_data: str,
    stop_event: threading.Event,
    sock: socket.socket,
    web_command_queue: queue.Queue,
) -> None:
    try:
        command = json.loads(received_data)
        if type(command) == str:
            raise TypeError
        command["socket"] = sock
        web_command_queue.put(command)
        # handle_if_command(command, stop_event, sock, web_command_queue)
    except json.JSONDecodeError as JDE:
        logger.error(
            f"{JDE}\n\nInvalid JSON format. Please provide valid JSON data.\n{received_data=}"
        )
    except TypeError as TE:
        logger.error(
            f"{TE}\n\nInvalid dictionary format. Please provide valid dictionary data.\n{received_data=}"
        )
    except Exception as e:
        logger.error(f"General Error:{e}")


@log_when_functions_start_and_stop
def start_server(
    host: str, port: int, stop_event: threading.Event, queue: queue.Queue
) -> None:
    local_logger = logger.getChild("webserver")
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.setblocking(0)  # type: ignore
        server_socket.bind((host, port))
        server_socket.listen(5)

        connected_clients = []

        while not stop_event.is_set():
            readable, _, _ = select.select(
                [server_socket] + connected_clients, [], [], 0.2
            )
            for sock in readable:
                if sock is server_socket:
                    # New connection, accept it
                    client_socket, client_address = sock.accept()
                    client_socket.setblocking(0)
                    local_logger.info(f"New connection from {client_address}")
                    connected_clients.append(client_socket)
                else:
                    # Data received from an existing client
                    data = receive_message(sock)
                    if data:
                        # print(f"Received data: {data.decode()}")
                        handle_received_data(
                            data.decode("utf-8"), stop_event, sock, queue
                        )
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
    stop_event.clear()

    web_server_thread = threading.Thread(
        target=start_server,
        args=(config.host, config.rx_port, stop_event, shared_web_command_queue),
    )

    handle_web_command_thread = threading.Thread(
        target=handle_commands,
        args=(shared_web_command_queue, display_queue, stop_event),
    )

    running_thread = threading.Thread(
        target=running_with_standard_file, args=(stop_event, display_queue)
    )

    # Start the threads
    web_server_thread.start()
    handle_web_command_thread.start()
    running_thread.start()

    try:
        while True:
            time.sleep(120)
            logger.getChild("main_loop").info("press ctrl+c to stop")
    except KeyboardInterrupt:
        stop_event.set()
        web_server_thread.join()
        handle_web_command_thread.join()
        running_thread.join()
    logger.info("Application Stopped")
