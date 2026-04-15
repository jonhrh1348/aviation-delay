FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x notebooks/run_all.sh

EXPOSE 8080

CMD ["bash", "notebooks/run_all.sh", "0.0.0.0:8080"]