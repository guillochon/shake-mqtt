FROM python:3.12-slim

WORKDIR /app

RUN useradd --create-home --uid 1000 appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shake_mqtt ./shake_mqtt

USER appuser

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "shake_mqtt"]
