Set-ExecutionPolicy -ExecutionPolicy Bypass
$env:FLASK_APP = "Middleware"
$env:FLASK_ENV = "development"
flask run