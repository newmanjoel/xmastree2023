import queue
import logging

import threading
import socket
import json


# networking imports
import socket
import select
import json


import common.common_send_recv as common_send_recv
from common.common_objects import (
    setup_common_logger,
    log_when_functions_start_and_stop,
)
from common.common_send_recv import send_message, receive_message


logger = logging.getLogger("networking")
logger = setup_common_logger(logger)


def confirm_and_handle_json_command(
    received_data: str,
    sock: socket.socket,
    command_queue: queue.Queue,
) -> None:
    try:
        command = json.loads(received_data)
        if type(command) == str:
            raise TypeError
        command["socket"] = sock
        command_queue.put(command)

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
def handle_networking(
    host: str,
    port: int,
    stop_event: threading.Event,
    command_queue: queue.Queue,
    send_queue: queue.Queue,
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
                        confirm_and_handle_json_command(
                            data.decode("utf-8"), sock, command_queue
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
