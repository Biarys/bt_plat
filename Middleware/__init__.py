import os
from flask import Flask
from flask import Flask, render_template
import pandas as pd
from flask import jsonify
from contextlib import contextmanager
import os
import numpy as np

# app = Flask(__name__)
# app.config["TEMPLATES_AUTO_RELOAD"] = True

# csvpath = os.path.abspath(os.path.join(os.getcwd(), "../stock_data/AAPL.csv"))
# df = pd.read_csv(csvpath)
# df["Date"] = pd.to_datetime(df["Date"]).astype(np.int64) / 1000000


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/')
    def home_page():
        return render_template('base.html')

    @app.route('/chart', endpoint='chart')
    def chart():
        df = pd.read_csv("stock_data\AA.csv")
        # df = df.to_json()
        return render_template('chart.html', data=df)

    @app.route('/debug', endpoint='debug')
    def debug():
        return render_template('debug.html')

    return app


# @app.route('/charts')
# def chart():
#     data = df.values.tolist()
#     return jsonify(data)
