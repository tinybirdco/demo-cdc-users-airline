from modules import tb_functions
from modules import utils

import pandas as pd
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go

logger = utils.setup_logging(print_to_console=True)

MAX_FLIGHTS_Y = 35
MAX_PASSENGER_VOLUME_Y = 2500
BAR_THICKNESS = .85  # Fixed thickness for the bars
REFRESH_INTERVAL_SECONDS = 2
PASSENGER_CHART_ROWS = 30

# Initialize the Dash app
logger.info("Initializing Dash app")
app = dash.Dash(__name__, assets_folder='assets')
app.title = "Flight Data Dashboard"
app._favicon = 'favicon.svg'

# Function to fetch and prepare data
def fetch_and_prepare_data(endpoint_name, time_col=None):
    logger.info(f"Fetching data from TinyBird endpoint: {endpoint_name}")
    resp = tb_functions.endpoint_fetch(endpoint_name=endpoint_name)
    if 'data' in resp:
        data_df = pd.DataFrame(resp['data'])
        logger.info(f"Data fetched successfully from {endpoint_name}. Shape: {data_df.shape}")
        if not data_df.empty and time_col:
            data_df[time_col] = pd.to_datetime(data_df[time_col])
    else:
        logger.error(f"Data fetch failed from {endpoint_name}. Returning empty DataFrame.")
        data_df = pd.DataFrame()
    return data_df

# Define the layout
app.layout = html.Div(children=[
    html.Div(children=[
        html.Img(src=app.get_asset_url('tinybird-logo.svg'), style={'height': '100px', 'margin-bottom': '20px'}),
        html.H1(children='Flight Data Dashboard', style={'margin-bottom': '20px'}),  # Added margin-bottom for spacing
        html.Div(children=[
            html.Div(children=[
                html.H2(children='Passenger States for Each Flight', className='subtitle', style={'margin-top': '20px'}),
                dcc.Graph(id='bar-chart-combined-states', style={'width': '100%', 'height': '100vh'})  # Full height
            ], style={'width': '50%', 'display': 'inline-block', 'vertical-align': 'top'}),
            html.Div(children=[
                html.Div(children=[
                    html.H2(children='Active Flights and Missed Passenger Percentages Over Time', className='subtitle'),
                    dcc.Graph(id='line-chart-active-vs-missed', style={'width': '100%', 'height': '20vh'}),
                ], style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}),
                html.Div(children=[
                    html.H2(children='Passenger Check-In and Completion Over Time', className='subtitle'),
                    dcc.Graph(id='line-chart-passenger-activity', style={'width': '100%', 'height': '20vh'}),
                ], style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}),
                html.Div(children=[
                    html.H2(children='Baggage Manifest Weights for Each Flight', className='subtitle'),
                    dcc.Graph(id='bar-chart-baggage', style={'width': '100%', 'height': '33vh'}),
                ], style={'width': '100%', 'display': 'inline-block', 'vertical-align': 'top'}),
            ], style={'width': '30%', 'display': 'inline-block', 'vertical-align': 'top'}),
        ], style={'width': '100%', 'display': 'inline-block'}),
        dcc.Interval(
            id='interval-component',
            interval=REFRESH_INTERVAL_SECONDS * 1000,  # Refresh interval in milliseconds (e.g., 2*1000ms = 2s)
            n_intervals=0
        ),
    ], style={'width': '100%', 'margin': '0 auto'})
], style={'textAlign': 'center'})

# Determine the color for the notcheckedin bar based on flight status
def determine_color(row):
    if row['flight_status'] == 'open':
        return '#d3d3d3'  # Grey
    else:
        return '#a3a3a3' # dark grey

@app.callback(
    [Output('line-chart-active-vs-missed', 'figure'),
     Output('line-chart-passenger-activity', 'figure'),
     Output('bar-chart-combined-states', 'figure'),
     Output('bar-chart-baggage', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_charts(n):
    logger.info(f"Updating charts with new data (n={n})")
    chart_refresh_start_time = pd.Timestamp.now()
    # Fetch data for active vs missed flights
    active_vs_missed_flights = fetch_and_prepare_data('active_vs_missed_flights.json', 'time_interval')

    # Create line chart for active vs missed flights
    if not active_vs_missed_flights.empty:
        logger.info(f"Updating active vs missed flights chart with {active_vs_missed_flights.shape[0]} rows")
        fig_active_vs_missed = go.Figure()
        fig_active_vs_missed.add_trace(go.Scatter(
            x=active_vs_missed_flights['time_interval'],
            y=active_vs_missed_flights['active_flights'],
            mode='lines',
            name='Active Flights',
            line=dict(color='blue')
        ))
        fig_active_vs_missed.add_trace(go.Scatter(
            x=active_vs_missed_flights['time_interval'],
            y=active_vs_missed_flights['flights_missed_pct'],
            mode='lines',
            name='Missed Passengers (%)',
            line=dict(color='red')
        ))
        fig_active_vs_missed.update_layout(
            xaxis=dict(),
            yaxis=dict(range=[0, MAX_FLIGHTS_Y]),  # Set appropriate y-axis range
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            ),
            margin=dict(t=0)
        )
    else:
        fig_active_vs_missed = go.Figure()
        fig_active_vs_missed.update_layout(
            title='No Data Available',
            yaxis=dict(range=[0, MAX_FLIGHTS_Y])  # Set the same range for empty data
        )

    # Fetch data for passenger activity
    passenger_activity = fetch_and_prepare_data('passenger_activity.json', 'interval')

    # Create line chart for passenger activity
    if not passenger_activity.empty:
        logger.info(f"Updating passenger activity chart with {passenger_activity.shape[0]} rows")
        fig_passenger_activity = go.Figure()
        fig_passenger_activity.add_trace(go.Scatter(
            x=passenger_activity['interval'],
            y=passenger_activity['passengers_checkedin'],
            mode='lines',
            name='Passengers Checked-In',
            line=dict(color='blue')
        ))
        fig_passenger_activity.add_trace(go.Scatter(
            x=passenger_activity['interval'],
            y=passenger_activity['passengers_completed'],
            mode='lines',
            name='Passengers Completed',
            line=dict(color='lime')
        ))
        fig_passenger_activity.update_layout(
            xaxis=dict(),
            yaxis=dict(range=[0, MAX_PASSENGER_VOLUME_Y]),  # Set appropriate y-axis range
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            ),
            margin=dict(t=0)
        )
    else:
        fig_passenger_activity = go.Figure()
        fig_passenger_activity.update_layout(
            title='No Data Available',
            yaxis=dict(range=[0, MAX_PASSENGER_VOLUME_Y])  # Set the same range for empty data
        )
    
    # Fetch data for combined passenger states
    combined_passenger_states = fetch_and_prepare_data('passengers_by_flight_status.json')

    # Create stacked bar chart for passenger states with horizontal bars
    if not combined_passenger_states.empty:
        # Use only max passenger rows
        combined_passenger_states = combined_passenger_states.head(PASSENGER_CHART_ROWS)

        # Adjust columns to handle missing values
        combined_passenger_states = combined_passenger_states.fillna(0)

        # Pad the DataFrame to ensure it has exactly X rows
        if combined_passenger_states.shape[0] < PASSENGER_CHART_ROWS:
            padding_rows = PASSENGER_CHART_ROWS - combined_passenger_states.shape[0]
            padding_df = pd.DataFrame({
                'flight_number': [''] * padding_rows,
                'flight_status': [''] * padding_rows,
                'checkedin': [0] * padding_rows,
                'boarding': [0] * padding_rows,
                'onboarded': [0] * padding_rows,
                'notboarded': [0] * padding_rows,
                'notcheckedin': [0] * padding_rows,
                'y_axis_label': [''] * padding_rows
            })
            combined_passenger_states = pd.concat([combined_passenger_states, padding_df], ignore_index=True)

        # Determine the color for the notcheckedin bar based on flight status
        combined_passenger_states['notcheckedin_color'] = combined_passenger_states.apply(determine_color, axis=1)

        # Create y-axis labels
        combined_passenger_states['y_axis_label'] = combined_passenger_states.apply(
            lambda row: f"{row['flight_number']} - {row['flight_status']}", axis=1
        )
        
        fig_combined_states = go.Figure()
        fig_combined_states.add_trace(go.Bar(
            y=combined_passenger_states['y_axis_label'],
            x=combined_passenger_states['notcheckedin'],
            name='Not Checked-In',
            marker=dict(color=combined_passenger_states['notcheckedin_color']),  # Lighter grey
            orientation='h'
        ))
        fig_combined_states.add_trace(go.Bar(
            y=combined_passenger_states['y_axis_label'],
            x=combined_passenger_states['checkedin'],
            name='Checked-In Passengers',
            marker=dict(color='#1f77b4'),
            orientation='h'
        ))
        fig_combined_states.add_trace(go.Bar(
            y=combined_passenger_states['y_axis_label'],
            x=combined_passenger_states['boarding'],
            name='Boarding Passengers',
            marker=dict(color='#17becf'),
            orientation='h'
        ))
        fig_combined_states.add_trace(go.Bar(
            y=combined_passenger_states['y_axis_label'],
            x=combined_passenger_states['onboarded'],
            name='Onboarded Passengers',
            marker=dict(color='green'),
            orientation='h'
        ))
        fig_combined_states.add_trace(go.Bar(
            y=combined_passenger_states['y_axis_label'],
            x=combined_passenger_states['notboarded'],
            name='Not Boarded',
            marker=dict(color='red'),
            orientation='h'
        ))
        fig_combined_states.update_layout(
            barmode='stack',
            showlegend=True,
            height=1300,
            margin=dict(t=0),
            xaxis=dict(range=[0, 350]),
            yaxis=dict(
                type='category',
                tickmode='array',
                tickvals=combined_passenger_states['y_axis_label'],
                ticktext=combined_passenger_states['y_axis_label'],
                tickangle=0,
                tickfont=dict(size=12)
            ),
            yaxis_autorange='reversed',  # To display in the same order as in the data
            autosize=False,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            )
        )
    else:
        fig_combined_states = go.Figure()
        fig_combined_states.update_layout(
            title='No Data Available'
        )
    
    # Fetch data for combined baggage states
    combined_baggage_states = fetch_and_prepare_data('baggage_by_flight_status.json')

    # Adjust columns to handle missing values
    combined_baggage_states = combined_baggage_states.fillna(0)

    # Create y-axis labels
    combined_baggage_states['y_axis_label'] = combined_baggage_states.apply(
        lambda row: f"{row['flight_number']} - {row['flight_status']}", axis=1
    )

    # Create stacked bar chart for baggage weights with horizontal bars
    if not combined_baggage_states.empty:
        
        fig_combined_baggage = go.Figure()
        fig_combined_baggage.add_trace(go.Bar(
            y=combined_baggage_states['y_axis_label'],
            x=combined_baggage_states['baggage_checkedin'],
            name='Checked-In Baggage',
            marker=dict(color='#1f77b4'),
            orientation='h'
        ))
        fig_combined_baggage.add_trace(go.Bar(
            y=combined_baggage_states['y_axis_label'],
            x=combined_baggage_states['baggage_loaded'],
            name='Loaded Baggage',
            marker=dict(color='#2ca02c'),
            orientation='h'
        ))
        fig_combined_baggage.add_trace(go.Bar(
            y=combined_baggage_states['y_axis_label'],
            x=combined_baggage_states['baggage_offloaded'],
            name='Offloaded Baggage',
            marker=dict(color='#d62728'),
            orientation='h'
        ))
        fig_combined_baggage.update_layout(
            barmode='stack',
            showlegend=True,
            height=600,
            margin=dict(t=0),
            xaxis=dict(title='Baggage Weight (kg)'),
            yaxis=dict(type='category'),
            yaxis_autorange='reversed',  # To display in the same order as in the data
            yaxis_tickmode='linear',
            yaxis_tickangle=0,
            autosize=False,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            )
        )
    else:
        fig_combined_baggage = go.Figure()
        fig_combined_baggage.update_layout(
            title='No Data Available'
        )

    chart_refresh_end_time = pd.Timestamp.now()
    logger.info(f"Charts updated in {(chart_refresh_end_time - chart_refresh_start_time).total_seconds() * 1000} milliseconds")

    return (fig_active_vs_missed, fig_passenger_activity, fig_combined_states, fig_combined_baggage)


if __name__ == '__main__':
    app.run_server(debug=True)
