FROM python:3.12-slim

WORKDIR /app

RUN mkdir -p /app/logs && \
    touch /app/logs/service.log && \
    chmod -R 777 /app/logs  # Права на запись для всех пользователей

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "./app/app.py"]
