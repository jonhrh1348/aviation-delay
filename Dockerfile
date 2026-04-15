FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    git \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN python -m ipykernel install --user

RUN chmod +x notebooks/run_all.sh

EXPOSE 8080

CMD ["bash", "notebooks/run_all.sh", "0.0.0.0:8080"]