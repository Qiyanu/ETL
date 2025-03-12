FROM gcr.io/serverless-runtimes/google-22-full/runtimes/python312

# Set environment variables to modify PATH and prevent warnings
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/root/.local/bin:${PATH}" \
    JOB_ID="default-job"

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Upgrade pip to the latest version and install dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
    --no-warn-script-location \
    -r requirements.txt

# Copy configuration and application code
COPY config.yaml .
COPY src/ ./src/

# Set Python path
ENV PYTHONPATH=/app \
    CONFIG_FILE=/app/config.yaml \
    K_SERVICE=customer-data-etl \
    LOG_LEVEL=INFO 
# Command to run
CMD ["python", "-m", "src.main"]