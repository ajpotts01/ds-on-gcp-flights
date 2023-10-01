FROM python:3.9.17-slim

COPY Pipfile /tmp/
COPY Pipfile.lock /tmp/

RUN pip install --trusted-host pypi.python.org pipenv
RUN python -m pipenv install

ENV ENV=PROD
EXPOSE 8080

RUN useradd --create-home app
WORKDIR /home/app
USER app

COPY ./src .
CMD start_server.sh