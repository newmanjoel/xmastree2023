import requests
import pandas as pd
import logging
import colorlog

logger = logging.getLogger('network_test')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
color_formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


def send_command(host, port, command, *args:list):
    if len(args) == 0:
        send_command_no_args(host, port, command)
    else:
        logger.debug(f'{command=},{args=}')
        send_command_with_args(host, port, command, args)

def send_command_with_args(host, port, command, args):
    url = f"http://{host}:{port}/{command}"
    response = requests.post(url, json=args)
    response.raise_for_status()

def send_command_no_args(host, port, command):
    logger.debug(f'no args with {command=}')
    url = f"http://{host}:{port}/{command}"
    response = requests.post(url)
    response.raise_for_status()
    

def send_test_dataframe(host, port):
    url = f"http://{host}:{port}/receivedf"
    data = {
        "calories": [420, 380, 390],
        "duration": [50, 40, 45]
    }
    test_df = pd.DataFrame(data)
    test_data = test_df.to_json()
    logger.debug(f'{type(test_data)=} | {test_data=}')
    response = requests.post(url, json=test_data)
    response.raise_for_status()
