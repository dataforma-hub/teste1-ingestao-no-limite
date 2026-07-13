#!/usr/bin/env python3
"""
Pipeline de ingestão — entrypoint do container.

O avaliador executa apenas:
  docker run <sua-imagem>

Sem argumentos CLI. Dados em /data/, config via env vars.
"""

from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Config (injetada pelo avaliador — não hardcode host/senha)
# ---------------------------------------------------------------------------

DATA_DIR = Path("/data")

PARTICIPANTE = os.environ["PARTICIPANTE"]
PG_TABLE = os.environ.get("PG_TABLE", f"{PARTICIPANTE}_empresas")

PG_HOST = os.environ.get("PG_HOST", "postgres_db")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_DB = os.environ.get("PG_DB", "db_empresas")

S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_BUCKET = os.environ.get("MINIO_BUCKET", "marketing-leads")
S3_PREFIX = f"{PARTICIPANTE}/"

PORTE_MAP = {
    "00": "NÃO INFORMADO",
    "01": "MICRO EMPRESA",
    "03": "EMPRESA DE PEQUENO PORTE",
    "05": "DEMAIS",
}

DDL = f"""
CREATE TABLE IF NOT EXISTS public."{PG_TABLE}" (
    cnpj_basico              VARCHAR(8) NOT NULL,
    razao_social             VARCHAR NOT NULL,
    natureza_juridica        VARCHAR(4) NOT NULL,
    qualificacao_responsavel VARCHAR NOT NULL,
    capital_social           DOUBLE PRECISION NOT NULL,
    porte_codigo             VARCHAR(2) NOT NULL,
    porte_descricao          VARCHAR NOT NULL,
    ente_federativo          VARCHAR
);
"""

# ---------------------------------------------------------------------------
# Helpers (implemente / ajuste conforme sua stack)
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def list_zip_files() -> list[Path]:
    zips = sorted(DATA_DIR.glob("*.zip"))
    if not zips:
        raise FileNotFoundError(f"Nenhum .zip encontrado em {DATA_DIR}")
    return zips


def iter_emprecsv_rows(zip_path: Path):
    """
    Lê linhas de arquivos *.EMPRECSV dentro do zip.
    Formato: ISO-8859-1, separador ';', aspas duplas, sem cabeçalho.
    """
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.upper().endswith(".EMPRECSV"):
                continue
            with zf.open(name) as raw:
                for line in raw:
                    text = line.decode("iso-8859-1").rstrip("\r\n")
                    # TODO: parse CSV com ';' e aspas — csv.reader ou polars
                    yield text


def transform_row(raw_fields: list[str]) -> dict | None:
    """
    raw_fields: colunas na ordem do arquivo .EMPRECSV (7 colunas de origem).

    Retorne None para descartar o registro (filtros B2B).
    """
    # TODO: mapear índices reais do CSV para cada campo
    # Exemplo ilustrativo — ajuste aos índices corretos:
    # cnpj_basico = raw_fields[0].zfill(8)
    # razao_social = raw_fields[1].strip().upper()
    # natureza_juridica = raw_fields[2].zfill(4)
    # qualificacao_responsavel = raw_fields[3]
    # capital_social = float(raw_fields[4].replace(".", "").replace(",", "."))
    # porte_codigo = raw_fields[5].zfill(2)
    # ente_federativo = raw_fields[6] or None

    raise NotImplementedError("Implemente transform_row() com os índices do CSV")


def passes_b2b_filters(row: dict) -> bool:
    """capital_social > 1000 e razao_social sem CPF de MEI no final."""
    if row["capital_social"] <= 1000.0:
        return False
    razao = row["razao_social"]
    tail = razao[-11:] if len(razao) >= 11 else ""
    if tail.isdigit() and len(tail) == 11:
        return False
    return True


def connect_postgres():
    import psycopg2

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def insert_batch(conn, rows: list[dict]) -> None:
    if not rows:
        return

    sql = f"""
        INSERT INTO public."{PG_TABLE}" (
            cnpj_basico, razao_social, natureza_juridica,
            qualificacao_responsavel, capital_social, porte_codigo,
            porte_descricao, ente_federativo
        ) VALUES (
            %(cnpj_basico)s, %(razao_social)s, %(natureza_juridica)s,
            %(qualificacao_responsavel)s, %(capital_social)s, %(porte_codigo)s,
            %(porte_descricao)s, %(ente_federativo)s
        )
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()


def run_pipeline() -> None:
    log("=== Ingestão no Limite — dataforma-hub ===")
    log(f"Participante : {PARTICIPANTE}")
    log(f"Tabela destino: public.{PG_TABLE}")
    log(f"Postgres     : {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    log(f"Dados brutos : {DATA_DIR}")

    zip_files = list_zip_files()
    log(f"Arquivos .zip: {len(zip_files)}")
    for path in zip_files:
        log(f"  - {path.name}")

    conn = connect_postgres()
    try:
        ensure_table(conn)

        batch: list[dict] = []
        batch_size = 10_000
        total = 0

        for zip_path in zip_files:
            log(f"Processando {zip_path.name}...")
            for line in iter_emprecsv_rows(zip_path):
                # TODO: substituir por parser real
                _ = line
                continue

                # Exemplo após parse:
                # raw_fields = parsed_fields
                # row = transform_row(raw_fields)
                # if row is None or not passes_b2b_filters(row):
                #     continue
                # row["porte_descricao"] = PORTE_MAP[row["porte_codigo"]]
                # batch.append(row)
                # if len(batch) >= batch_size:
                #     insert_batch(conn, batch)
                #     total += len(batch)
                #     batch.clear()
                #     log(f"  {total:,} linhas gravadas...")

        if batch:
            insert_batch(conn, batch)
            total += len(batch)

        log(f"Concluído — {total:,} linhas em public.{PG_TABLE}")

        if total == 0:
            log("ERRO: nenhuma linha gravada.")
            sys.exit(1)

    finally:
        conn.close()


def main() -> int:
    try:
        run_pipeline()
        return 0
    except NotImplementedError as exc:
        log(f"Pipeline incompleto: {exc}")
        return 1
    except Exception as exc:
        log(f"ERRO: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
