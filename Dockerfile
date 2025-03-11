FROM gcr.io/serverless-runtimes/google-22-full/runtimes/python312

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy configuration and application code
COPY config.yaml .
COPY main.py .

# Command to run
CMD ["python", "main.py"]
