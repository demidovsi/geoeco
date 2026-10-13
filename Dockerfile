FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /code

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py cacert.pem ./

# main_geo_server.py обрабатывает SIGTERM от docker stop и пишет в лог shutdown
CMD ["python", "main_geo_server.py"]
