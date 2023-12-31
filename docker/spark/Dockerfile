# https://github.com/bitnami/containers/tree/main/bitnami/spark
FROM docker.io/bitnami/spark:3.4.1-debian-11-r26

ENV PYTHON_VERSION=3.11.5
ENV APP_PATH=/app

USER root
WORKDIR $APP_PATH

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y curl htop build-essential gdb lcov pkg-config libbz2-dev \
    libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev libncurses5-dev \
    libreadline6-dev libsqlite3-dev libssl-dev lzma lzma-dev tk-dev uuid-dev zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Build python from source
RUN mkdir /opt/python && mkdir /opt/python/python-$PYTHON_VERSION \ 
    && curl https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz --output /opt/python/python-$PYTHON_VERSION/source.tgz \
    && tar xvf /opt/python/python-$PYTHON_VERSION/source.tgz -C /opt/python/python-$PYTHON_VERSION \
    && cd /opt/python/python-$PYTHON_VERSION/Python-$PYTHON_VERSION \
    && ./configure --enable-optimizations --prefix=/opt/python/python-$PYTHON_VERSION \
    && make -j 8 && make altinstall \
    && rm -rf /opt/python/python-$PYTHON_VERSION/source.tgz && rm -rf /opt/python/python-$PYTHON_VERSION/Python-$PYTHON_VERSION

ENV PATH="/opt/python/python-$PYTHON_VERSION/bin:$PATH" \
    PYTHONPATH=/opt/python/python-$PYTHON_VERSION/bin/python3.11

# Install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_HOME=/opt/poetry /opt/python/python-$PYTHON_VERSION/bin/python3.11 -

ENV PATH="/opt/poetry/bin:$PATH"

# Download certificate for yandex-cloud
RUN curl "https://storage.yandexcloud.net/cloud-certs/CA.pem" -o $APP_PATH/CA.pem
RUN chmod 644 $APP_PATH/CA.pem

# Install dependensies
COPY ./pyproject.toml $APP_PATH/pyproject.toml

RUN cd $APP_PATH \
    && poetry config virtualenvs.in-project true \
    && poetry env use /opt/python/python-$PYTHON_VERSION/bin/python3.11 \
    && source $APP_PATH/.venv/bin/activate \
    && poetry install --only spark