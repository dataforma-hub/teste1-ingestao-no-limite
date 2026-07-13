FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# O avaliador executa apenas: docker run <sua-imagem>
# Portanto o CMD deve disparar TODA a ingestão até gravar no Postgres.
CMD ["python", "src/main.py"]
