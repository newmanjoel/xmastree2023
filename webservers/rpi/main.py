import socket
import json
import logging
import colorlog

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
pixels = neopixel.NeoPixel(board.D10, 50)
pixels.fill((100,100,100))
pixels.show()

def handle_fill(args):
    color_r = int(args[0])
    color_g = int(args[1])
    color_b = int(args[2])
    data = (color_g, color_r, color_b)
    logger.getChild('fill').info(f'filling with {data=}')
    pixels.fill(data)
    pixels.show()


def handle_command(command):
    # Define the logic to handle different commands
    logger.debug(f"{command=}")
    target_command = command["command"]
    match target_command:
        case "fill":
            handle_fill(command["args"])
        case "off":
            handle_fill([0,0,0])
        case "single":
            pass
        case "list":
            pass
        case _:
            pass
    

  

def start_server(host, port):
    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            try:
                server_socket.bind((host, port))
                server_socket.listen()

                logger.info(f"Server listening on {host}:{port}")

                while True:
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
                            handle_command(command)
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

    start_server(host, port)