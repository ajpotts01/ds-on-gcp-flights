import flask
import functions_framework
from markupsafe import escape

import transforms


def main() -> str:
    try:
        transforms.main()
        return "Success"
    except Exception as ex:
        return f"Error: {ex}"


@functions_framework.http
def transform_flights(request):
    result: str = main()
    return result


if __name__ == "__main__":
    result: str = main()
    print(result)
