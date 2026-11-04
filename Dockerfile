FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DATA_DIR=/data

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY bridge.py .

RUN useradd --uid 1000 --create-home bridge \
 && mkdir -p /data && chown bridge:bridge /data
USER bridge
VOLUME ["/data"]

ENTRYPOINT ["python", "bridge.py"]
CMD ["run"]
