FROM python:3.9-slim-buster

# Copy requirements file
COPY requirements.txt requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Copy application code into container
COPY . .

# Expose application port
EXPOSE 8000

# Start application
CMD ["python", "app.py"]
