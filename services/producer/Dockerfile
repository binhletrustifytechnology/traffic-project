FROM python:3.11-slim

WORKDIR /app

COPY producer.py /app/
RUN pip install kafka-python

CMD ["python", "producer.py"]
