from datetime import datetime
from pathlib import Path
from dash import Dash, dcc, html, Input, Output
from numpy import size
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import common_objects as co
import file_parser as fp


app = Dash(__name__)

coordinate_file_path = Path(
    r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\fixed_coords_2021.gift"
)

test_sequence_path = Path(
    r"C:\Users\joell\OneDrive\Documents\GitHub\xmastree2023\examples\rainbow_sine.csv"
)
led_sequence = fp.read_from_csv(test_sequence_path)

sequence_df = led_sequence.convert_to_df(include_led_column=False)


# get all the locations
led_locations, df = fp.read_GIFT_file(coordinate_file_path)

xdata, ydata, zdata = co.get_xyz_from_locations(led_locations)

all_info_melted = co.all_info_for_plotting(led_locations, led_sequence)
all_info = co.get_all_info_in_df(led_locations, led_sequence)


# df = px.data.gapminder()
# print(sequence_df.melt(id_vars=["led_id"], var_name="frame_id", value_name="led_color"))

# print(sequence_df)

print(f"reloaded data at {datetime.now()}")


frames = []
for frame in sequence_df.T.itertuples(index=False):
    # print(frame)
    local_color = list(frame)
    frames.append(
        go.Frame(
            data=go.Scatter3d(
                x=xdata,
                y=ydata,
                z=zdata,
                mode="markers",
                marker=dict(color=local_color),
            )
        )
    )

# print(frames)

manual_ani_fig = go.Figure(
    data=[go.Scatter3d(x=xdata, y=ydata, z=zdata, mode="markers")],
    frames=frames,
    layout=go.Layout(
        title="Start Title",
        updatemenus=[
            dict(
                type="buttons",
                buttons=[
                    dict(
                        label="Play",
                        method="animate",
                        args=[
                            None,
                            {
                                "frame": {
                                    "duration": 50,
                                    "redraw": True,
                                },
                                "transition": {
                                    "duration": 30,
                                    "easing": "cubic-in-out",
                                },
                            },
                        ],
                    ),
                    dict(
                        label="Pause",
                        method="animate",
                        args=[
                            [None],
                            {
                                "frame": {
                                    "duration": 0,
                                    "redraw": True,
                                },
                                "transition": {
                                    "duration": 0,
                                    "easing": "cubic-in-out",
                                },
                            },
                        ],
                    ),
                ],
            )
        ],
    ),
)
# all_slider_steps = []
# for index, row in sequence_df.T.iterrows():
#     slider_step = {
#         "args": [
#             [index],
#             {
#                 "frame": {"duration": 300, "redraw": True},
#                 "mode": "immediate",
#                 "transition": {"duration": 300},
#             },
#         ],
#         "label": f"{index}",
#         "method": "animate",
#     }
#     all_slider_steps.append(slider_step)

# sliders_dict = {
#     "active": 0,
#     "yanchor": "top",
#     "xanchor": "left",
#     "currentvalue": {
#         "font": {"size": 20},
#         "prefix": "Frame:",
#         "visible": True,
#         "xanchor": "right",
#     },
#     "transition": {"duration": 300, "easing": "cubic-in-out"},
#     "pad": {"b": 10, "t": 50},
#     "len": 0.9,
#     "x": 0.1,
#     "y": 0,
#     "steps": all_slider_steps,
# }

# manual_ani_fig.update_layout(
#     {
#         "updatemenus": [
#             {
#                 "buttons": [
#                     {
#                         "args": [
#                             None,
#                             {
#                                 "frame": {"duration": 100, "redraw": True},
#                                 "fromcurrent": True,
#                                 "transition": {
#                                     "duration": 100,
#                                     "easing": "quadratic-in-out",
#                                 },
#                             },
#                         ],
#                         "label": "Play",
#                         "method": "animate",
#                     },
#                     {
#                         "args": [
#                             [None],
#                             {
#                                 "frame": {"duration": 0, "redraw": True},
#                                 "mode": "immediate",
#                                 "transition": {"duration": 0},
#                             },
#                         ],
#                         "label": "Pause",
#                         "method": "animate",
#                     },
#                 ],
#                 "direction": "left",
#                 "pad": {"r": 10, "t": 87},
#                 "showactive": False,
#                 "type": "buttons",
#                 "x": 0.1,
#                 "xanchor": "right",
#                 "y": 0,
#                 "yanchor": "top",
#             }
#         ]
#     }
# )

# manual_ani_fig.update_layout({"sliders": [sliders_dict]})


app.layout = html.Div(
    [
        html.H4("Display tree in 3D coordinates"),
        dcc.Graph(
            id="3D_christmas_tree",
            figure=manual_ani_fig,
            style={"width": "90vw", "height": "90vh"},
        ),
    ]
)


# @app.callback(Output("3D_christmas_tree", "figure"), Input("range-slider", "value"))
# def update_bar_chart(slider_range):
#     df = px.data.iris()  # replace with your own data source
#     low, high = slider_range
#     mask = (df.petal_width > low) & (df.petal_width < high)

#     fig = px.scatter_3d(
#         df[mask],
#         x="sepal_length",
#         y="sepal_width",
#         z="petal_width",
#         color="species",
#         hover_data=["petal_width"],
#     )
#     return fig


app.run_server(debug=True)
