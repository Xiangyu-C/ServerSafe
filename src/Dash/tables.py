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

cluster = Cluster(['ec2-18-232-2-76.compute-1.amazonaws.com'])
cass_session = cluster.connect('cyber_id')

df = pd.DataFrame(list(cass_session.execute('select * from cyber_ml')))

total_benign = df['true_label'].value_counts()['Benign']
total_malicious = len(df)-total_benign
benign_predicted = df[(df['prediction']==0) & (df['true_label']=='Benign')].count()
malicious_predicted = df[(df['prediction']==1) & (df['true_label']!='Benign')].count()
benign_rate = str(round((benign_predicted/total_benign)[0],2))
malicious_rate = str(round((malicious_predicted/total_malicious)[0], 2))

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background'], 'color': colors['text']}, children=[
    html.H1(children='Cyber Attack Dashboard'),
    dcc.Graph(id='live-update-graph-attack'),
    dcc.Graph(id='live-update-graph-traffic'),
    html.Div(html.Table(id='live-update-table'
    [
        html.Tr( [html.Th("Traffic Class"), html.Th("Prediction Accuracy"), html.Th('Total count')] )
    ] +
    [
        html.Tr( [html.Td("Benign"), html.Td(benign_rate), html.Td(total_benign)] ),
        html.Tr( [html.Td("Malicious"), html.Td(malicious_rate), html.Td(total_malicious)] )
    ]
),
),
    dcc.Interval(
        id='interval-component',
        interval=1*1000, # in milliseconds
        n_intervals=0)
])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=80)
