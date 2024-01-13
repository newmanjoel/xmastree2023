# generic helpful things
import logging
import time


# used for multi-threading
import threading
import queue
from io import StringIO


# used for being able to import stuff from other folders
import os
import sys

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)


# used for sharing data between modules
import config


from common.common_objects import setup_common_logger
from commands import handle_commands
from networking import handle_networking
from display import show_data_on_leds


logger = logging.getLogger("light_driver")
logger = setup_common_logger(logger)

# set up the capture on the root logger so it capture everything
log_capture = StringIO()
logging.getLogger().addHandler(logging.StreamHandler(log_capture))

config.log_capture = log_capture


# set up the queues and events
stop_event = threading.Event()
stop_event.clear()
display_queue = queue.Queue()
command_queue = queue.Queue()
send_queue = queue.Queue()

# put something in the command_queue right away so that it can boot to something
command_queue.put(
    {
        "command": "loadfile",
        "args": "/home/pi/github/xmastree2023/examples/rainbow-implosion.csv",
    }
)


if __name__ == "__main__":
    total_running_time_s = 0
    stop_event.clear()

    web_server_thread = threading.Thread(
        target=handle_networking,
        args=(config.host, config.rx_port, stop_event, command_queue, send_queue),
    )

    command_thread = threading.Thread(
        target=handle_commands,
        args=(command_queue, display_queue, send_queue, stop_event),
    )

    running_thread = threading.Thread(
        target=show_data_on_leds, args=(stop_event, display_queue)
    )

    # Start the threads
    web_server_thread.start()
    command_thread.start()
    running_thread.start()

    try:
        while not stop_event.is_set():
            time.sleep(1)
            total_running_time_s += 1
            if total_running_time_s % 10 == 0:
                string_to_check = "press ctrl+c to stop\n"
                log_contents = config.log_capture.getvalue()
                log_contents = log_contents[-50:]
                logger.debug(
                    f"{log_contents=} {string_to_check=} {log_contents.endswith(string_to_check)=}"
                )
                # if not config.log_capture.getvalue().endswith("press ctrl+c to stop"):
                logger.getChild("main_loop").info("press ctrl+c to stop")
                config.log_capture.truncate(100_000)
    except KeyboardInterrupt:
        stop_event.set()
        web_server_thread.join()
        command_thread.join()
        running_thread.join()
    logger.info("Application Stopped")
