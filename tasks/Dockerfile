FROM python:3.10-slim

# Set environment variables for best practices in Docker
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies (gcc might be needed for some libraries)
RUN apt-get update && apt-get install -y --no-install-recommends gcc

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- THE FIX: Copy ONLY the necessary application files ---
COPY app.py .
COPY ingest_streaming_reddit.py .
COPY hot_path_processor.py .
# ---------------------------------------------------------

# Expose the Streamlit port so we can access it from our browser
EXPOSE 8501

# The default command to run. We will override this for our backend scripts.
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]