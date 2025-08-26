FROM python:3.10-slim

RUN apt-get update

WORKDIR /app

COPY . /app

EXPOSE 8000

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "sync.py"]
