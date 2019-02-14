import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly
from dash.dependencies import Input, Output
from cassandra.cluster import Cluster
import pandas as pd
import dash_table
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
    # Put the two graphs side by side
    html.Div([html.Div([dcc.Graph(id='live-update-graph-attack')], className='six columns'),
              html.Div([dcc.Graph(id='live-update-graph-traffic')], className='six columns'),
             ], className='row'),
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
    # Create the graph with subplots
    fig = {
    'data': [go.Bar(x=data['attacks'],
                    y=data['servers'],
                    orientation='h')],
    'layout': {
        'height': 400,
        'title': 'Predicted attacks per second by server',
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

    clrred = 'rgb(222,0,0)'
    clrblue = 'rgb(31,119,180)'
    clrs  = [clrred if x >300 else clrblue for x in data['traffic']]

    # Create the graph with subplots
    fig = {
    'data': [go.Bar(x=data['traffic'],
                    y=data['servers'],
                    marker=dict(color=clrs),
                    orientation='h')],
    'layout': {
        'height': 400,
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
    df = pd.DataFrame(list(cass_session.execute('select * from all_predictions')))
    df['predicted_labels'] = df['prediction'].map(labels_dict)
    top_6 = df['Label'].value_counts()[0:6]
    top_6_labels = top_6.index.tolist()
    all_top_6_data = df[df['predicted_labels'].isin(top_6_labels)]
    correct_predictions = all_top_6_data[all_top_6_data['predicted_labels']==all_top_6_data['Label']]
    true_predictions_count = []
    for label in top_6_labels:
        true_predictions_count.append(correct_predictions['Label'].value_counts()[label])
    prediction_accuracy = [str(round(true_predictions_count[i]*100/top_6[i], 1))+'%' for i in range(6)]
    top_6_counts = top_6.tolist()
    prediction_accuracy.insert(0, 'Accuracy')
    top_6_counts.insert(0, 'Total')
    top_6_labels.insert(0, 'Traffic Class')
    df1 = pd.DataFrame([prediction_accuracy, top_6_counts], columns=top_6_labels)

    return dash_table.DataTable(
           id='table',
           columns=[{"name": i, "id": i} for i in df1.columns], #['Traffic Class', 'Prediction Accuracy', 'Total Count']],
           data=df1.to_dict("rows"),
           style_header={'backgroundColor': 'rgb(30, 30, 30)',
                         'font-size': '180%',
                         'fontWeight': 'bold',
                         'textAlign': 'center',
                         'color': 'rgb(127, 219, 255)'},
           style_cell={
               'backgroundColor': 'rgb(50, 50, 50)',
               'color': 'white',
               'textAlign': 'center',
               'font-size': '150%'
           }
         )

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=80)
