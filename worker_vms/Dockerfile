# worker_vms/Dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Expose Flask port and Prometheus port
EXPOSE 5001 5002 8000
CMD ["python", "worker_node.py"]