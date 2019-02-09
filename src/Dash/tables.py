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
server_list = ['168.16.4.175',
               '192.0.1.242',
               '192.31.198.84',
               '192.31.216.211',
               '192.82.127.93',
               '198.51.105.72',
               '198.51.122.45',
               '198.51.19.176',
               '201.122.184.249',
               '203.0.121.119',
               '203.79.63.201',
               '4.41.219.116',
               '94.172.102.187']

labels_dict = {0: 'Benign',
               1: 'DDOS attack-HOIC',
               2: 'DDoS attacks-LOIC-HTTP',
               3: 'DoS attacks-Hulk',
               4: 'Bot',
               5: 'FTP-BruteForce',
               6: 'SSH-Bruteforce',
               7: 'Infilteration',
               8: 'DoS attacks-SlowHTTPTest',
               9: 'DoS attacks-GoldenEye',
              10: 'DoS attacks-Slowloris',
              11: 'Brute Force -Web',
              12: 'DDOS attack-LOIC-UDP'}

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background'], 'color': colors['text']}, children=[
    html.H1(style={'textAlign': 'center'}, children='Server Safe Monitor Dashboard'),
    dcc.Graph(id='live-update-graph-attack'),
    dcc.Graph(id='live-update-graph-traffic'),
    html.Div(id='live-update-table'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000, # in milliseconds
        n_intervals=0)
])

@app.callback(Output('live-update-graph-attack', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_attack_live(n):
    data = {
        'servers': server_list,
        'attacks': []
    }

    rows = cass_session.execute('select * from count_attack limit 1')
    for row in rows:
        data['attacks'].extend([row.a, row.b, row.c, row.d, row.e, row.f, row.g,
                               row.h, row.i, row.j, row.k, row.l, row.m])
    data['attacks'] =  [x/50 for x in data['attacks']]
    # Create the graph with subplots
    fig = {
    'data': [go.Bar(x=data['attacks'],
                    y=data['servers'],
                    orientation='h')],
    'layout': {
        'title': 'Predicted attacks per second by server',
        #'width': 500,
        'xaxis': {
            'title': '# of predicted attacks per second'
        },
        'yaxis': {
            'title': 'Server IPs',
            'automargin': True
        }
    }
    }

    return fig

@app.callback(Output('live-update-graph-traffic', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_traffic_live(n):
    data = {
        'servers': server_list,
        'traffic': []
    }

    rows = cass_session.execute('select * from count_traffic limit 1')
    for row in rows:
        data['traffic'].extend([row.a, row.b, row.c, row.d, row.e, row.f, row.g,
                               row.h, row.i, row.j, row.k, row.l, row.m])

    # Create the graph with subplots
    fig = {
    'data': [go.Bar(x=data['traffic'],
                    y=data['servers'],
                    orientation='h')],
    'layout': {
        'title': 'Total traffic per second by server',
        'xaxis': {
            'title': 'Total traffic (# of requests) per second'
        },
        'yaxis': {
            'title': 'Server IPs',
            'automargin': True
        }
    }
    }

    return fig


@app.callback(Output('live-update-table', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_table_live(n):
    df = pd.DataFrame(list(cass_session.execute('select * from cyber_predictions')))
    df['predicted_labels'] = df['predictions'].map(labels_dict)
    total_benign = df['label'].value_counts()['Benign']
    total_malicious = len(df)-total_benign
    benign_predicted = df[(df['prediction']==0) & (df['label']=='Benign')].count()
    malicious_predicted = df[(df['prediction']==1) & (df['label']!='Benign')].count()
    benign_rate = str(round((benign_predicted/total_benign)[0],2))
    malicious_rate = str(round((malicious_predicted/total_malicious)[0], 2))

    return html.Table(
    [
        html.Tr( [html.Th("Traffic Class"), html.Th("Prediction Accuracy"), html.Th('Total count')] )
    ] +
    [
        html.Tr( [html.Td("Benign"), html.Td(benign_rate), html.Td(total_benign)] ),
        html.Tr( [html.Td("Malicious"), html.Td(malicious_rate), html.Td(total_malicious)] )
    ]
)

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=80)
