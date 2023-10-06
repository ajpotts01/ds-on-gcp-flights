import flask
import functions_framework
from markupsafe import escape

import transforms


@functions_framework.http
def transform_flights():
    transforms.main()
