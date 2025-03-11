FROM gcr.io/serverless-runtimes/google-22-full/runtimes/python312

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy configuration and application code
COPY config.yaml .
COPY src/ ./src/

# Set Python path
ENV PYTHONPATH=/app

# Ensure container has a JOB_ID for proper identification
ENV JOB_ID=${JOB_ID:-default-job}

# Command to run
CMD ["python", "-m", "src.main"]