from flask import Flask, render_template
import pandas as pd
from flask import jsonify
from contextlib import contextmanager
import os
import numpy as np

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

# csvpath = os.path.abspath(os.path.join(os.getcwd(), "../stock_data/AAPL.csv"))
# df = pd.read_csv(csvpath)
# df["Date"] = pd.to_datetime(df["Date"]).astype(np.int64) / 1000000


@app.route('/')
def home_page():
    return render_template('home.html')


# @app.route('/charts')
# def chart():
#     data = df.values.tolist()
#     return jsonify(data)
