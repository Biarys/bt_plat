from flask import Flask, render_template
import pandas as pd
from flask import jsonify
from contextlib import contextmanager
import os


app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

csvpath = os.path.abspath(os.path.join(os.getcwd(), "../Data/AAPL.csv"))
df = pd.read_csv(csvpath)

@app.route('/')
def hello_world():
    return render_template('test.html')


@app.route('/charts')
def chart():
    data=df.values.tolist()
    return jsonify(data)
