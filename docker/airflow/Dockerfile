FROM apache/airflow:2.7.1-python3.11

WORKDIR /opt/airflow
USER root

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl htop \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow


# # Install poetry
# RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry /opt/python/python-$PYTHON_VERSION/bin/python3.11 -

# # Install dependensies
# COPY ./pyproject.toml $APP_PATH/pyproject.toml