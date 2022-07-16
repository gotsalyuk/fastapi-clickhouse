FROM python:3.9

ENV LANG ru_RU.UTF-8
ENV LANGUAGE ru
ENV PYTHONUNBUFFERED 1


WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .