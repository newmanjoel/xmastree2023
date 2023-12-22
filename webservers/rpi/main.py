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

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)

from common.common_send_recv import send_message, receive_message
from common.common_objects import setup_common_logger, all_standard_column_names


logger = logging.getLogger("light_driver")
logger = setup_common_logger(logger)


# set up global variables that will be shared accros the threads
led_num = 500
pixels = neopixel.NeoPixel(board.D12, led_num, auto_write=False)
pixels.fill((100, 100, 100))
pixels.show()

fps = 10
stop_event = threading.Event()
stop_event.clear()

shared_queue = queue.Queue()


lock = threading.Lock()

column_names = all_standard_column_names(led_num)

# Create DataFrame filling with black
current_df_sequence = pd.DataFrame(0, index=range(1), columns=column_names)

shared_queue.put(current_df_sequence)


def handle_get_logs(args, sock: socket.socket):
    running_path = Path(".")
    # csv_file_path = Path("/home/pi/github/xmastree2023/examples")
    running_path.absolute()
    files = list(map(str, list(running_path.glob("*"))))
    files.append(str(running_path.absolute()))

    send_message(sock, json.dumps(files).encode("utf-8"))


def handle_fill(args, queue: queue.Queue):
    # converts RGB into a GRB hex
    if type(args) != list:
        logger.getChild("fill").error(
            f"trying to fill with something that is not a list {type(args)=}\n{args=}"
        )
        return
    if len(args) != 3:
        logger.getChild("fill").error(
            f"trying to fill with more than 3 elements {len(args)=}\n{args=}"
        )
        return
    color_r = int(args[0])
    color_g = int(args[1])
    color_b = int(args[2])
    data = [color_g, color_r, color_b] * led_num
    with lock:
        current_df_sequence = pd.DataFrame([data], index=range(1), columns=column_names)
        queue.put(current_df_sequence)


def handle_one(args, queue: queue.Queue):
    # converts RGB into a GRB hex
    if type(args) != list:
        logger.getChild("fill").error(
            f"trying to fill with something that is not a list {type(args)=}\n{args=}"
        )
        return
    if len(args) != 4:
        logger.getChild("fill").error(
            f"trying to fill with more than 3 elements {len(args)=}\n{args=}"
        )
        return
    index = int(args[0])
    color_r = int(args[1])
    color_g = int(args[2])
    color_b = int(args[3])

    data = [0, 0, 0] * led_num
    data[index] = color_r
    data[index + 1] = color_g
    data[index + 2] = color_b

    with lock:
        current_df_sequence = pd.DataFrame([data], index=range(1), columns=column_names)
        queue.put(current_df_sequence)


def handle_fps(args):
    global fps
    try:
        args = float(args)
    except Exception:
        # this is bad, but IDC I check the type later
        pass
    if type(args) == int or type(args) == float:
        fps = args
    else:
        logger.getChild("fps").warning(
            f"Tried to set the FPS to {args=}, this needs to be a number."
        )


def handle_file(args, queue: queue.Queue):
    # load a csv file
    # load that into a dataframe
    # check that it has the right size
    # check that each element is a hex code
    local_logger = logger.getChild("handle_file")

    if type(args) != str:
        local_logger.error(f"needed a file path, got {type(args)=}, {args=}")
        return
    file_path = Path(args)
    if not file_path.exists():
        local_logger.error(f"File dosn't exist. {file_path=}")
        return

    results = None

    start = time.time()
    results = pd.read_csv(file_path)
    end = time.time()
    local_logger.debug(f"loaded the file to a dataframe and it took {end-start:0.3f}")

    local_logger.debug(
        f"loaded the file to a dataframe and it is using {results.memory_usage(deep=True).sum()}b"
    )
    with lock:
        current_df_sequence = results
        queue.put(current_df_sequence)


def handle_brightness(args) -> None:
    local_logger = logger.getChild("brightness")
    if type(args) != int and type(args) != float:
        local_logger.error(f"need a float, got {type(args)=}, {args=}")
        return
    if float(args) > 1 or float(args) < 0:
        local_logger.error(f"brightness out of bounds. Needs to be between 0 and 1")
        return
    brightness = float(args)
    pixels.brightness = brightness


def handle_add_list(args, queue: queue.Queue) -> None:
    global current_df_sequence, led_num
    raise NotImplementedError
    if type(args) == list:
        pass
    else:
        logger.getChild("add_list").warning(
            f"needed a list, but got {type(args)} of {args=}"
        )
        return

    if len(args) != led_num:
        logger.getChild("add_list").warning(
            f"needed a list of len({led_num}), but got {len(args)} of {args=}"
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


def handle_getting_list_of_files(args, sock: socket.socket) -> None:
    """Return a list of the current CSV's that can be played"""
    csv_file_path = Path("/home/pi/github/xmastree2023/examples")
    csv_files = list(map(str, list(csv_file_path.glob("*.csv"))))

    send_message(sock, json.dumps(csv_files).encode("utf-8"))


def handle_getting_temp(args, sock: socket.socket) -> None:
    """measure the temperature of the raspberry pi"""
    import subprocess

    result = subprocess.run(
        ["vcgencmd", "measure_temp"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    json_string = json.dumps({"temp": str(result.stdout)})
    send_message(sock, json_string.encode("utf-8"))
    logger.getChild("temp").debug(f"Sent back {json_string}")


def handle_if_command(
    command: dict, stop_event: threading.Event, sock: socket.socket, queue: queue.Queue
) -> None:
    # Define the logic to handle different commands
    logger.debug(f"{command=}")
    target_command = command.get("command", "error")
    if target_command == "fill":
        handle_fill(command["args"], queue)
    elif target_command == "off":
        handle_fill([0, 0, 0], queue)
    elif target_command == "single":
        handle_one(command["args"], queue)
        pass
    elif target_command == "list":
        # handle_list(command['args'])
        pass
    elif target_command == "addlist":
        # handle_add_list(command["args"])
        pass
    elif target_command == "get_log":
        handle_get_logs(command["args"], sock)
    elif target_command == "show_df":
        handle_show_df(command["args"], sock, queue)
    elif target_command == "loadfile":
        handle_file(command["args"], queue)
        pass
    elif target_command == "brightness":
        handle_brightness(command["args"])
    elif target_command == "get_list_of_files":
        handle_getting_list_of_files(command["args"], sock)
    elif target_command == "temp":
        handle_getting_temp(command["args"], sock)
    elif target_command == "fps":
        handle_fps(command["args"])
    elif target_command == "pause":
        handle_fps(0)
    elif target_command == "stop":
        stop_event.set()
    elif target_command == "error":
        logger.getChild("handle_commands").error(
            f"Caught a dictionary error. The command parameter is invalid. {command=}"
        )


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
        row_list = [None] * led_num

        for pixel_num in range(led_num):
            row_list[pixel_num] = (  # type: ignore
                row[f"G_{pixel_num}"],
                row[f"R_{pixel_num}"],
                row[f"B_{pixel_num}"],
            )

        results[index] = row_list  # type: ignore
    local_logger.debug("ending conversion")
    # local_logger.debug(f"\n{results}")
    return results  # type: ignore


@log_when_functions_start_and_stop
def running_with_standard_file(
    stop_event: threading.Event, working_queue: queue.Queue
) -> None:
    global pixels, led_num
    local_logger = logger.getChild("running")
    working_df = current_df_sequence
    fast_array = convert_df_to_list_of_tuples(working_df)
    while not stop_event.is_set():
        if not working_queue.empty():
            try:
                working_df = working_queue.get()
                local_logger.info("Changing to new df")
                fast_array = convert_df_to_list_of_tuples(working_df)
            except queue.Empty as e:
                pass

        for row in fast_array:
            if stop_event.is_set() or not working_queue.empty():
                break
            time1 = time.time()
            for pixel_num in range(led_num):
                # local_logger.debug(f"Trying to set id {pixel_num} to {row}")
                color = row[pixel_num]
                pixels[pixel_num] = (color[0], color[1], color[2])
                # local_logger.debug(f"set id {pixel_num} to {row}")
            time2 = time.time()
            pixels.show()
            time3 = time.time()
            loop_time = time3 - time1
            fps_time = 1.0 / fps
            sleep_time = fps_time - loop_time
            if sleep_time < 0:
                sleep_time = 0
            while fps == 0:
                time.sleep(0.5)
            else:
                time.sleep(sleep_time)
            time4 = time.time()
            # local_logger.debug(
            #     f"Loading Array:{time2-time1:.3f}s Pushing Pixels:{time3-time2:.3f}s sleeping:{time4-time3:.3f}s actual_FPS:{1/(time4-time1):.3f}"
            # )


def handle_received_data(
    received_data: str,
    stop_event: threading.Event,
    sock: socket.socket,
    queue: queue.Queue,
) -> None:
    try:
        command = json.loads(received_data)
        # logger.getChild("received_data").debug(f"{type(command)=} {command=}")
        if type(command) == str:
            raise TypeError
        handle_if_command(command, stop_event, sock, queue)
    except json.JSONDecodeError as JDE:
        logger.error(
            f"{JDE}\n\nInvalid JSON format. Please provide valid JSON data.\n{received_data=}"
        )
    except TypeError as TE:
        logger.error(
            f"{TE}\n\nInvalid dictionary format. Please provide valid dictionary data.\n{received_data=}"
        )


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
    host = "192.168.4.205"
    port = 12345  # Change this to the desired port number
    stop_event.clear()

    web_server_thread = threading.Thread(
        target=start_server, args=(host, port, stop_event, shared_queue)
    )

    running_thread = threading.Thread(
        target=running_with_standard_file, args=(stop_event, shared_queue)
    )

    # Start the threads
    web_server_thread.start()
    running_thread.start()

    try:
        while True:
            time.sleep(120)
            logger.getChild("main_loop").info("press ctrl+c to stop")
    except KeyboardInterrupt:
        stop_event.set()
        web_server_thread.join()
        running_thread.join()
    logger.info("Application Stopped")
