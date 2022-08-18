FROM python:3.10.2

LABEL maintainer="Jose-Maria Vazquez-Jimenez"
RUN apt-get update

# Set timezone, not really necessary...
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install basic python packages: setuptools, pip3, virtualenv
RUN pip3 install --upgrade setuptools && pip3 install pip virtualenv

# For Oracle Databases...
RUN apt-get install unzip && apt-get install libaio1
RUN mkdir -p /opt/oracle
ADD oracle/instantclient-basic-linux.x64-21.7.0.0.0dbru.zip /opt/oracle/
RUN cd /opt/oracle/  \
    && unzip instantclient-basic-linux.x64-21.7.0.0.0dbru.zip  \
    && rm instantclient-basic-linux.x64-21.7.0.0.0dbru.zip
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_7

# Let's go with poetry...
ENV POETRY_VERSION=1.1.13 VENV_PATH="/code/.venv" POETRY_HOME="/opt/poetry"
RUN curl -sSL https://install.python-poetry.org | python3 - --version $POETRY_VERSION
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

VOLUME /code
WORKDIR /code
