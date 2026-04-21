FROM python:3.12-slim

WORKDIR /app

# Dependencias del sistema para Pillow
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar fuentes
COPY . .

# NODE_ID toma el hostname del contenedor (único por réplica en Docker)
ENV NODE_ID=""
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
