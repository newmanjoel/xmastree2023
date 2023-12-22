import logging
import socket

from common.common_objects import setup_common_logger


logger = logging.getLogger("common")
logger = setup_common_logger(logger)


def receive_message(client_socket: socket.socket) -> bytes:
    # Assuming the first 8 bytes represent the length of the message
    # logger.getChild("recv").debug(
    #     f"Getting the first 8 bytes to tell how big things are"
    # )
    message_length_bytes = client_socket.recv(8)
    message_length = int.from_bytes(message_length_bytes, byteorder="big")

    # logger.getChild("recv").debug(f"{message_length=}")

    received_data = b""
    remaining_bytes = message_length

    while remaining_bytes > 0:
        chunk_size = min(
            4096, remaining_bytes
        )  # Adjust the chunk size based on your needs
        chunk = client_socket.recv(chunk_size)
        if not chunk:
            # Connection closed prematurely
            break
        received_data += chunk
        remaining_bytes -= len(chunk)
    # logger.getChild("recv").debug(f"Finished Receiving")
    return received_data


def send_message(server_socket: socket.socket, message: bytes) -> None:
    # Disable Nagle algorithm
    server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # Send the length of the message as the first 8 bytes
    message_length = len(message)
    message_length_bytes = message_length.to_bytes(8, byteorder="big")
    server_socket.sendall(message_length_bytes)

    # logger.getChild("send").debug(f"{message_length=}")
    # message = message_length_bytes + message

    # Send the message in chunks
    chunk_size = 4096  # Adjust the chunk size based on your needs
    offset = 0
    while offset < message_length:
        end_offset = min(offset + chunk_size, message_length)
        server_socket.sendall(message[offset:end_offset])
        offset = end_offset
    # logger.getChild("send").debug(f"Finished Sending")
