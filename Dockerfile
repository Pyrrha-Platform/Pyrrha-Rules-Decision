FROM registry.access.redhat.com/ubi8/python-38:1-71
EXPOSE 8080

ENV PYTHONUNBUFFERED=1

COPY --chown=1001 requirements.txt .
COPY --chown=1001 manage.py .

RUN pip install -r /opt/app-root/src/requirements.txt

COPY --chown=1001 src/* ./

RUN [ -f ".env" ] || cp .env.docker .env

ENTRYPOINT [ "python" ]
CMD ["manage.py", "start", "0.0.0.0:3000"]

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD curl -f http://localhost:8080/health
