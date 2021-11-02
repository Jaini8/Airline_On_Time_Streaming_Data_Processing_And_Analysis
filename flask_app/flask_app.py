from flask import Flask, request, render_template, jsonify, url_for
import pandas as pd

import subprocess
import signal
import os
import socket
from collections import deque


app = Flask(__name__)
data = None
spark_process = None
buffer = deque()


def preprocess_carrier_punctuality_data(df):
    df['Year'] = pd.to_numeric(df['Year'])
    # df['Month'] = pd.to_numeric(df['Month'])
    return df


def postprocess_carrier_punctuality_data(df):
    res = df.fillna(0.0).rename(columns={'UniqueCarrier': 'name', 'punctuality': 'value'})
    res['value'] = res['value'] * 100.0
    return res.to_json(orient="records")


def preprocess_worst_months_data(df):
    df['Year'] = pd.to_numeric(df['Year'])
    df['Month'] = pd.to_numeric(df['Month'])
    return df

def postprocess_worst_months_data(df):
    res = df.fillna(0.0).rename(columns={'non_punctuality': 'value'})
    res['value'] = res['value'] * 100.0
    # sort to make amchart happy
    res = res.sort_values(by=['Month'], ascending=False)
    return res.to_json(orient="records")

def preprocess_busy_airports_data(df):
    df['Year'] = pd.to_numeric(df['Year'])
    # df['Month'] = pd.to_numeric(df['Month'])
    return df

def postprocess_busy_airports_data(df):
    res = df.fillna(0.0).rename(columns={'total_count': 'value'})
    return res.to_json(orient="records")

def preprocess_busy_paths_data(df):
    df['Year'] = pd.to_numeric(df['Year'])
    # df['Month'] = pd.to_numeric(df['Month'])
    return df

def postprocess_busy_paths_data(df):
    res = df.rename(columns={'total_flights': 'Total Flights', 'origin_name': 'Origin Name', 'airport': 'Destination Name', 'Dest': 'Dsetination'})
    return res.to_html(index=False)


query_func_mapping = {
    'carrier_punctuality' : (preprocess_carrier_punctuality_data, postprocess_carrier_punctuality_data),
    'worst_months': (preprocess_worst_months_data, postprocess_worst_months_data),
    'busiest_airports': (preprocess_busy_airports_data, postprocess_busy_airports_data),
    'busy_paths': (preprocess_busy_paths_data, postprocess_busy_paths_data)
}
preprocess_func = None
postprocess_func = None



@app.route("/set_data", methods=['POST'])
def set_data():
    global buffer
    global preprocess_func
    assert preprocess_func is not None
    payload = request.json
    curr_data = pd.io.json.read_json(payload, orient='split')
    curr_data = preprocess_func(curr_data)
    print(len(curr_data))

    buffer.append(curr_data)
    return jsonify(success=True)


@app.route('/get_data')
def get_data():
    global buffer
    global postprocess_func
    
    if len(buffer) == 0:
        return jsonify([])
    else:
        assert postprocess_func is not None
        data = buffer.pop()
        return postprocess_func(data)

    
@app.route('/start')
def start():
    global spark_process
    global preprocess_func
    global postprocess_func
    if spark_process is not None:
        pid = spark_process.pid
        os.kill(pid, signal.SIGTERM)

    query_name = request.args.get('query_name')
    spark_script_filename = f'stream_{query_name}.py'
    spark_process = subprocess.Popen(
        ['spark-submit', spark_script_filename], 
        stdout=subprocess.DEVNULL
    )

    preprocess_func, postprocess_func = query_func_mapping[query_name]
    query_js_filename = f'render_{query_name}.js'
    return jsonify(success=True, js_link=url_for('static', filename=query_js_filename))
    

@app.route('/')
def home():
    global data
    data = None
    return render_template('index.html', host_name=socket.gethostname())


if __name__ == '__main__':
    app.run(host=socket.gethostname(), port=9996)
