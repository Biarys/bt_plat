from flask import Flask, render_template
import pandas as pd
from flask import template_rendered
from contextlib import contextmanager

app = Flask(__name__)

df = pd.read_csv("/media/Storage/Ubuntu/Default/Desktop/bt_plat/Data/AAPL.csv", index_col="Date")

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/charts')
def chart():
    data=df.to_html()
    return render_template('test.html', data=data)

@contextmanager
def captured_templates(app):
    recorded = []
    def record(sender, template, context, **extra):
        recorded.append((template, context))
    template_rendered.connect(record, app)
    try:
        yield recorded
    finally:
        template_rendered.disconnect(record, app)
