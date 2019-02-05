import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
from cassandra.cluster import Cluster
import pandas as pd
import plotly.plotly as py
import plotly.graph_objs as go


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background'], 'color': colors['text']}, children=[
    html.Div(html.Table(
    [
        html.Tr( [html.Th("Traffic Class"), html.Th("Prediction Accuracy"), html.Th('Total count')] )
    ] +
    [
        html.Tr( [html.Td("Benign"), html.Td(benign_rate), html.Td(total_benign)] ),
        html.Tr( [html.Td("Malicious"), html.Td(malicious_rate), html.Td(total_malicious)] )
    ]
)
)
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=80)
