Set-ExecutionPolicy Bypass -File .\start_flask.ps1
$env:FLASK_APP = "Middleware"
$env:FLASK_ENV = "development"
flask run