from pathlib import Path
import pandas as pd
import numpy as np


def all_standard_column_names(num: int) -> list[str]:
    results = []
    for i in range(num):
        results.append(f"R_{i}")
        results.append(f"G_{i}")
        results.append(f"B_{i}")
    return results


led_num = 500

column_names = all_standard_column_names(led_num)

# Create DataFrame filling with black
current_df_sequence = pd.DataFrame([], columns=column_names)

print(f"{current_df_sequence}")

color_array = [0] * led_num * 3
color_array[0] = 255

for i in range(led_num):
    working_array = np.roll(color_array, shift=i * 3, axis=0)
    working_df = pd.DataFrame(data=[working_array], columns=column_names)
    # print(f"{working_df=}")
    current_df_sequence = pd.concat(
        [current_df_sequence, working_df],
        axis=0,
        ignore_index=True,
    )
    # current_df_sequence.loc[i] = working_array

current_df_sequence.to_csv(
    Path(
        r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\examples\chasing_lights.csv"
    )
)

print(f"{current_df_sequence}")
