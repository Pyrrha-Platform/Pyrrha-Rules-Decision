FROM python:3.7.7-slim

ENV PYTHONUNBUFFERED=1

COPY src/* /opt/microservices/src/
COPY manage.py /opt/microservices/
COPY requirements.txt /opt/microservices/

# hadolint ignore=DL3008,DL3013,DL3015
RUN pip install --upgrade pip \
  && pip install --upgrade pipenv\
  && apt-get update \
  && apt-get install --no-install-recommends -y python \
  && apt-get install -y build-essential \
  && apt-get install -y libmariadb3 libmariadb-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && pip install --upgrade -r /opt/microservices/requirements.txt

EXPOSE 8080
WORKDIR /opt/microservices/

ENTRYPOINT [ "python" ]
CMD ["manage.py", "start", "0.0.0.0:3000"]