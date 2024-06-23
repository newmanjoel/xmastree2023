from io import StringIO


fps: float = 10
show_fps: bool = False
frame_rate_arr: list[float] = []


host: str = "192.168.2.39"
rx_port: int = 12345
tx_port: int = 12346
log_capture: StringIO = StringIO()


led_num: int = 500
led_pin: int = 12
brightness: float = 1.0
pixels = {}  # I dont like this
current_dataframe = {}  # I dont like this
fast_array = {}  # I dont like this
