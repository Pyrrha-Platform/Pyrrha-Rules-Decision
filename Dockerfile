FROM registry.access.redhat.com/ubi8/python-38:1-71
EXPOSE 8080

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/microservices/

COPY requirements.txt .
RUN pip install -r /opt/microservices/requirements.txt

COPY src/* .
RUN [ -f ".env" ] || cp .env.docker .env

ENTRYPOINT ["python", "core_decision_flask_app.py", "8080"]