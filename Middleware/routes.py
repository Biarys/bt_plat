from . import app
from flask import render_template

@app.route("/")
@app.route("/index")
def index():
    return render_template("base.html")

@app.route("/chart")
def chart():
    return render_template("chart.html")

@app.route("/debug")
def debug():
    return render_template("debug.html")