import board
import neopixel
import time

led_num = 500
pixels = neopixel.NeoPixel(
    board.D12, led_num, bpp=3, auto_write=False, pixel_order=neopixel.GRB
)
import numpy as np

black = "#090909"

pixels.fill((100, 100, 100))
pixels.show()


lighting = [black] * led_num

clamp = lambda n, minn, maxn: max(min(maxn, n), minn)

fade_amount = 20
multi_amount = 255 / fade_amount
fps = 30.0


def rgb_to_hex(r, g, b):
    """Convert RGB values to hex color code."""
    hex_color = f"#{r:02X}{g:02X}{b:02X}"
    return hex_color


def hex_to_rgb(hex_color):
    """Convert hex color code to RGB values."""
    hex_color = hex_color.lstrip("#")  # Remove '#' if present
    rgb = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    return rgb


for x in range(fade_amount):
    red = int(clamp(x * multi_amount, 0, 255))
    lighting[x] = rgb_to_hex(0, red, 0)

actual_FPS = 0
loading_time = 0
changing_time = 0
sleeping_time = 0
try:
    while True:
        time1 = time.time()
        for pixel_num in range(led_num):
            pixels[pixel_num] = hex_to_rgb(lighting[pixel_num])
        # pixels[0:led_num] = lighting
        time2 = time.time()
        lighting = list(np.roll(lighting, 1, axis=0))
        time3 = time.time()
        pixels.show()
        time.sleep(1 / fps)
        time4 = time.time()
        actual_FPS = time4 - time1
        loading_time = time2 - time1
        changing_time = time3 - time2
        sleeping_time = time4 - time3


# fill is GRB?
finally:
    print(f"Actual FPS is {1/actual_FPS:0.3f}")
    print(f"Loading time is {loading_time:0.4f}")
    print(f"Changing time is {changing_time:0.4f}")
    print(f"Sleeping time is {sleeping_time:0.4f}")
    pixels.fill((0, 0, 0))
    pixels.show()
