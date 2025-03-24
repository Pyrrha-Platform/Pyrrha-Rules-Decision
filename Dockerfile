FROM python:3.8.12-bullseye as build

WORKDIR /usr/app
RUN python -m venv /usr/app/venv
ENV PATH="/usr/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.8.12-slim-bullseye@sha256:d31a1beb6ccddbf5b5c72904853f5c2c4d1f49bb8186b623db0b80f8c37b5899

RUN apt-get update &&\
    apt-get --no-install-recommends -y install curl=7.74.* &&\
    rm -rf /var/lib/apt/lists/* &&\
    groupadd -g 999 python &&\
    useradd -u 999 -g python python &&\
    mkdir /usr/app &&\
    chown python:python /usr/app

WORKDIR /usr/app

EXPOSE 8080
ENV PYTHONUNBUFFERED=1

COPY --from=build --chown=python:python /usr/app/venv ./venv
ENV PATH="/usr/app/venv/bin:$PATH"

WORKDIR /usr/app/src
COPY --chown=python:python src/* ./ 
RUN [ -f ".env" ] || cp .env.docker .env

USER python

ENTRYPOINT ["gunicorn"  , "-b", "0.0.0.0:8080", "core_decision_flask_app:app"]
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD curl -f http://localhost:8080/health
