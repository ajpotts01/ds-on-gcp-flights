FROM python:3.9.17-slim

COPY Pipfile /tmp/
COPY Pipfile.lock /tmp/

ENV ENV=PROD
ENV PIPENV_PIPFILE=/tmp/Pipfile
ENV PORT=8080

RUN pip install --trusted-host pypi.python.org pipenv
RUN python -m pipenv install --system

EXPOSE 8080

RUN useradd --create-home app
WORKDIR /home/app

COPY ./src .
RUN chmod +x ./start_server.sh
USER app
CMD ./start_server.sh