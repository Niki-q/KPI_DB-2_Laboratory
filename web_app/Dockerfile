# app/Dockerfile

FROM python:3.9-slim

RUN mkdir /app
WORKDIR /app

# Copy requirements file
COPY requirements.txt /app/

# Install dependencies
RUN pip install -r requirements.txt

# Copy application code into container
COPY . /app/

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]