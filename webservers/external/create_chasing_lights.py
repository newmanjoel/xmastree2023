from pathlib import Path
import time
import pandas as pd
import numpy as np

# used for being able to import stuff from other folders
import os
import sys

# Add the root directory to the Python path
current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)

from common.common_objects import all_standard_column_names

led_num = 500
column_names = all_standard_column_names(led_num)


# Create DataFrame filling with black
current_df_sequence = pd.DataFrame([], columns=column_names)

print(f"{current_df_sequence}")


color_array = [0] * led_num * 3
color_ammount = 255
fade_ammount = 1.0
for i in range(0, 80, 3):
    color_array[i] = int(color_ammount * fade_ammount)
    fade_ammount = fade_ammount * 0.8
rolling_array = np.copy(color_array)
sequence_start = time.time()
for i in range(led_num + 25):
    total_shift = i * 3
    rolling_array = np.roll(rolling_array, shift=3, axis=0)
    working_df = pd.DataFrame(data=[rolling_array], columns=column_names)
    # print(f"{working_df=}")
    current_df_sequence = pd.concat(
        [current_df_sequence, working_df],
        axis=0,
        ignore_index=True,
    )
    # current_df_sequence.loc[i] = working_array
sequence_end = time.time()
current_df_sequence.index.name = "FRAME_ID"
print(f"it took {sequence_end-sequence_start:0.3f}s to create the sequence")
file_path = Path(
    r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\examples\chasing_lights.csv"
)
current_df_sequence.to_csv(file_path)

print(f"{current_df_sequence}")
print(f"{file_path=}")
