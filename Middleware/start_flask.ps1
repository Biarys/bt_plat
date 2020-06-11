# Set-ExecutionPolicy Bypass -File .\start_flask.ps1
$env:FLASK_APP = "Middleware/main.py"
$env:FLASK_ENV = "development"
flask run