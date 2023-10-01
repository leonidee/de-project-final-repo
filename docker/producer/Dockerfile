FROM python:3.11.4-bullseye

ENV APP_PATH=/app

USER root
WORKDIR $APP_PATH

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry $(which python3) -

ENV PATH="/opt/poetry/bin:$PATH"

# Download certificate for yandex-cloud
RUN curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" -o $APP_PATH/CA.pem
RUN chmod +r $APP_PATH/CA.pem

COPY ./pyproject.toml /app/pyproject.toml
COPY ./src /app/src
COPY ./.env /app/.env


RUN cd /app \
    && poetry env use $(which python3) \
    && . $(poetry env info --path)/bin/activate \
    && poetry install --only producer-app


ENTRYPOINT ["/bin/sh", "-c"]
CMD [". $(poetry env info --path)/bin/activate && python /app/src/producer/runner.py"]