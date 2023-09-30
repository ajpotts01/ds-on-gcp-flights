# The book uses a Python 3.8 image.
# Instead, use Ubuntu, similar to the CI builds.
FROM ubuntu:22.04

COPY Pipfile /tmp/
COPY Pipfile.lock /tmp/

RUN apt-get update && apt-get install -y wget
RUN apt-get install -y python3.9
RUN pip install --trusted-host pypi.python.org pipenv
RUN python -m pipenv install

ENV ENV=PROD

RUN useradd --create-home app
WORKDIR /home/app
USER app

COPY ./data_extract/src .
CMD start_server.sh