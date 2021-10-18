FROM registry.access.redhat.com/ubi8/python-38@sha256:65b1acc755bb9e73286a1b73e0683172082ef5034d92e38a55566e4ac5e0ab47
USER 1001
WORKDIR /opt/app-root/src/

EXPOSE 8080
ENV PYTHONUNBUFFERED=1

COPY --chown=1001 requirements.txt .
RUN pip install --no-cache-dir -r /opt/app-root/src/requirements.txt

COPY --chown=1001 src/* ./

RUN [ -f ".env" ] || cp .env.docker .env

ENTRYPOINT ["gunicorn"  , "-b", "0.0.0.0:8080", "core_decision_flask_app:app"]
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD curl -f http://localhost:8080/health
