import logging
import socket

from common.common_objects import setup_common_logger


logger = logging.getLogger("common")
logger = setup_common_logger(logger)

verbose: bool = True
default_chunk_size = 4096


def receive_message(client_socket: socket.socket) -> bytes:
    # Assuming the first 8 bytes represent the length of the message
    if verbose:
        logger.getChild("recv").debug(
            f"Getting the first 8 bytes to tell how big things are"
        )
    message_length_bytes = client_socket.recv(8)
    message_length = int.from_bytes(message_length_bytes, byteorder="big")

    if verbose:
        logger.getChild("recv").debug(f"{message_length=}")

    received_data = b""
    remaining_bytes = message_length

    while remaining_bytes > 0:
        # Adjust the chunk size based on your needs
        chunk_size = min(default_chunk_size, remaining_bytes)
        chunk = client_socket.recv(chunk_size)
        if verbose:
            logger.getChild("recv").debug(
                f"receved  [{chunk_size}:{remaining_bytes}] out of {message_length}"
            )
        if not chunk:
            # Connection closed prematurely
            break
        received_data += chunk
        remaining_bytes -= len(chunk)
    if verbose:
        logger.getChild("recv").debug(f"Finished Receiving")
    return received_data


def send_message(server_socket: socket.socket, message: bytes) -> None:
    # Disable Nagle algorithm
    server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    # Send the length of the message as the first 8 bytes
    message_length = len(message)
    message_length_bytes = message_length.to_bytes(8, byteorder="big")
    server_socket.sendall(message_length_bytes)

    if verbose:
        logger.getChild("send").debug(f"{message_length=}")
    # message = message_length_bytes + message

    # Send the message in chunks
    offset = 0
    while offset < message_length:
        end_offset = min(offset + default_chunk_size, message_length)
        server_socket.sendall(message[offset:end_offset])
        if verbose:
            logger.getChild("send").debug(
                f"sent chunk [{offset}:{end_offset}] out of {message_length}"
            )
        offset = end_offset
    if verbose:
        logger.getChild("send").debug(f"Finished Sending")
