import flask
import functions_framework
from markupsafe import escape


@functions_framework.http
def transform_flights():
    flask.request
