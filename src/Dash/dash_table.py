import datetime

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
import plotly.plotly as py
import plotly.graph_objs as go


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.layout = html.Div(
    html.Div(html.Table(
    [
        html.Tr( [html.Th("name"), html.Th("address"), html.Th("email")] )
    ] +
    [
        html.Tr( [html.Td("walter"), html.Td("rudin"), html.Td("wr@analysis.com")] ),
        html.Tr( [html.Td("gilbert"), html.Td("strang"), html.Td("gb@algebra.com")] )
    ]
)
)
)

if __name__ == '__main__':
    app.run_server(debug=True)
