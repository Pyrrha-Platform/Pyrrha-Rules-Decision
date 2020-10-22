FROM python:3.7.7-slim

ENV PYTHONUNBUFFERED=1

COPY src/* /opt/microservices/
COPY requirements.txt /opt/microservices/
RUN pip install --upgrade pip \
  && pip install --upgrade pipenv\
  && apt-get update \
  && apt install -y build-essential \
  && apt install -y libmariadb3 libmariadb-dev \
  && pip install --upgrade -r /opt/microservices/requirements.txt

EXPOSE 8080
WORKDIR /opt/microservices/

CMD ["python", "core_decision_flask_app.py", "8080"]