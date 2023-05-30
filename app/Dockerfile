FROM python:3.9-slim-buster

ENV PYTHONUNBUFFERED 1

RUN mkdir /app
WORKDIR /app

# Copy requirements file
COPY requirements.txt /app/

# Install dependencies
RUN pip install -r requirements.txt

# Copy application code into container
COPY . /app/

# Start application
CMD ["python", "app.py"]
