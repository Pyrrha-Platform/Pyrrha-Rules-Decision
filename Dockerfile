FROM python:3.7.7-slim

ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/* /opt/microservices/
COPY requirements.txt /opt/microservices/

EXPOSE 8080
WORKDIR /opt/microservices/

CMD ["python", "core_decision_flask_app.py", "8080"]