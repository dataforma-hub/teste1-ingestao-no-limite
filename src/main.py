#!/usr/bin/env python3
"""
Pipeline de ingestão — entrypoint do container (avaliação oficial).

Lê /data/*.zip → transforma → grava public.{PG_TABLE}
"""

from __future__ import annotations

import csv
import io
import os
import re
import zipfile
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

DATA_DIR = Path("/data")
BATCH_SIZE = 5_000

PARTICIPANTE = os.environ["PARTICIPANTE"]
PG_TABLE = os.environ.get("PG_TABLE", f"{PARTICIPANTE}_empresas")
PG_HOST = os.environ.get("PG_HOST", "postgres_db")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_DB = os.environ.get("PG_DB", "db_empresas")

PORTE_MAP = {
    "00": "NÃO INFORMADO",
    "01": "MICRO EMPRESA",
    "03": "EMPRESA DE PEQUENO PORTE",
    "05": "DEMAIS",
}

CPF_TAIL = re.compile(r"\d{11}$")

CAPITAL_FAIXA_MAP = [
    (0, 0, "SEM CAPITAL"),
    (0, 1000, "ATÉ 1K"),
    (1000, 10000, "1K A 10K"),
    (10000, 100000, "10K A 100K"),
    (100000, 1000000, "100K A 1M"),
    (1000000, float("inf"), "ACIMA DE 1M"),
]

NJ_GRUPO_MAP = {
    "1": "ADMINISTRAÇÃO PÚBLICA",
    "2": "ENTIDADES EMPRESARIAIS",
    "3": "ENTIDADES SEM FINS LUCRATIVOS",
    "4": "PESSOAS FÍSICAS",
    "5": "ORGANIZAÇÕES INTERNACIONAIS",
}


def get_capital_faixa(capital: float) -> str:
    for lo, hi, label in CAPITAL_FAIXA_MAP:
        if lo < capital <= hi:
            return label
    return "SEM CAPITAL"


def get_natureza_grupo(natureza: str) -> str:
    return NJ_GRUPO_MAP.get(natureza[0], "OUTROS")


def is_mei(razao: str) -> bool:
    return bool(CPF_TAIL.search(razao))


def log(msg: str) -> None:
    print(msg, flush=True)


def parse_capital(raw: str) -> float:
    s = (raw or "").strip()
    if not s:
        return 0.0
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def transform_row(fields: list[str], data_processamento: str) -> tuple | None:
    if len(fields) < 7:
        return None

    cnpj = re.sub(r"\D", "", fields[0]).zfill(8)[-8:]
    if len(cnpj) != 8 or not cnpj.isdigit():
        return None

    razao = (fields[1] or "").strip().upper()
    if razao is None:
        return None

    natureza = re.sub(r"\D", "", fields[2] or "").zfill(4)[-4:]
    if len(natureza) != 4 or not natureza.isdigit():
        return None

    qualificacao = (fields[3] or "").strip()
    if not qualificacao:
        return None

    capital = parse_capital(fields[4])

    porte = re.sub(r"\D", "", fields[5] or "").zfill(2)[-2:]
    if porte not in PORTE_MAP:
        porte = "00"

    ente = (fields[6] or "").strip()
    ente_clean = ente if ente else None

    capital_faixa = get_capital_faixa(capital)
    is_mei_flag = is_mei(razao)
    nj_grupo = get_natureza_grupo(natureza)
    ente_presente = ente_clean is not None

    return (
        cnpj,
        razao,
        natureza,
        qualificacao,
        capital,
        porte,
        PORTE_MAP[porte],
        ente_clean,
        capital_faixa,
        is_mei_flag,
        nj_grupo,
        ente_presente,
        data_processamento,
    )


def iter_rows():
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat()

    zips = sorted(DATA_DIR.glob("*.zip"))
    if not zips:
        raise FileNotFoundError(f"Nenhum .zip em {DATA_DIR}")

    log(f"Arquivos .zip: {len(zips)}")
    for zp in zips:
        log(f"  - {zp.name}")

    for zp in zips:
        log(f"Processando {zp.name}...")
        with zipfile.ZipFile(zp) as zf:
            for name in zf.namelist():
                if not name.upper().endswith(".EMPRECSV"):
                    continue
                with zf.open(name) as raw:
                    text = io.TextIOWrapper(raw, encoding="iso-8859-1", newline="")
                    reader = csv.reader(text, delimiter=";", quotechar='"')
                    for fields in reader:
                        row = transform_row(fields, ts)
                        if row is not None:
                            yield row


def connect():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )


def prepare_table(conn) -> None:
    ddl = f"""
    DROP TABLE IF EXISTS public."{PG_TABLE}";
    CREATE TABLE public."{PG_TABLE}" (
        cnpj_basico                  VARCHAR(8) NOT NULL,
        razao_social                 VARCHAR NOT NULL,
        natureza_juridica             VARCHAR(4) NOT NULL,
        qualificacao_responsavel      VARCHAR NOT NULL,
        capital_social               DOUBLE PRECISION NOT NULL,
        porte_codigo                 VARCHAR(2) NOT NULL,
        porte_descricao              VARCHAR NOT NULL,
        ente_federativo              VARCHAR,
        capital_social_faixa         VARCHAR NOT NULL,
        is_mei                       BOOLEAN NOT NULL,
        natureza_juridica_grupo      VARCHAR NOT NULL,
        ente_federativo_presente     BOOLEAN NOT NULL,
        data_processamento           TIMESTAMP NOT NULL
    );
    CREATE UNIQUE INDEX idx_cnpj_unique ON public."{PG_TABLE}" (cnpj_basico);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def flush_batch(conn, batch: list[tuple]) -> None:
    if not batch:
        return
    sql = f"""
        INSERT INTO public."{PG_TABLE}" (
            cnpj_basico, razao_social, natureza_juridica,
            qualificacao_responsavel, capital_social, porte_codigo,
            porte_descricao, ente_federativo,
            capital_social_faixa, is_mei, natureza_juridica_grupo,
            ente_federativo_presente, data_processamento
        ) VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, batch, page_size=BATCH_SIZE)
    conn.commit()


def main() -> int:
    log("=== Ingestão no Limite ===")
    log(f"Participante : {PARTICIPANTE}")
    log(f"Tabela destino: public.{PG_TABLE}")
    log(f"Postgres     : {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    log(f"Dados brutos : {DATA_DIR}")

    conn = connect()
    try:
        prepare_table(conn)

        batch: list[tuple] = []
        total = 0

        for row in iter_rows():
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                flush_batch(conn, batch)
                total += len(batch)
                batch.clear()
                if total % 100_000 == 0:
                    log(f"  {total:,} linhas gravadas...")

        flush_batch(conn, batch)
        total += len(batch)

        log(f"Concluído — {total:,} linhas em public.{PG_TABLE}")
        if total == 0:
            log("ERRO: nenhuma linha gravada.")
            return 1
        return 0
    except Exception as exc:
        log(f"ERRO: {exc}")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
