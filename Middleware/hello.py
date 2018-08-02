from flask import Flask, render_template
import pandas as pd
from flask import jsonify
from contextlib import contextmanager
import os


app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

csvpath = os.path
df = pd.read_csv(r"C:\Users\Biarys\Desktop\bt_plat\Data\AAPL.csv")

@app.route('/')
def hello_world():
    return render_template('test.html')


@app.route('/charts')
def chart():
    data=df.values.tolist()
    return jsonify(data)
