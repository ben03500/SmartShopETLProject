FROM python:3.12.4

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt