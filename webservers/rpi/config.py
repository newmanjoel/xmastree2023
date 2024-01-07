from io import StringIO


fps: float = 10
show_fps: bool = False


host: str = "192.168.4.205"
rx_port: int = 12345
tx_port: int = 12346
log_capture: StringIO = StringIO()


led_num: int = 500
brightness: float = 1.0
pixels = {}  # I dont like this
current_dataframe = {}  # I dont like this
