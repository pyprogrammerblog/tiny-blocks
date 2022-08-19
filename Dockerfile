FROM python:3.10.2

LABEL maintainer="Jose-Maria Vazquez-Jimenez"
RUN apt-get update

# Set timezone, not really necessary...
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install basic python packages: setuptools, pip3, virtualenv
RUN pip3 install --upgrade setuptools && pip3 install pip virtualenv

# Let's go with poetry...
ENV POETRY_VERSION=1.1.13 VENV_PATH="/code/.venv" POETRY_HOME="/opt/poetry"
RUN curl -sSL https://install.python-poetry.org | python3 - --version $POETRY_VERSION
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

VOLUME /code
WORKDIR /code
