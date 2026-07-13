#!/usr/bin/env python3
"""
Testa o pipeline localmente como o avaliador faz:
  docker build → docker run (2 CPU, 2 GB, /data montado)

Uso:
  export PG_PASSWORD='sua_senha'
  python test_local.py --data-dir /caminho/para/zips

Opcional — conferir tabela no Postgres depois:
  python test_local.py --data-dir /caminho/zips --check-db
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


def load_config() -> dict:
    path = REPO_ROOT / "participante.json"
    if not path.exists():
        sys.exit(f"Arquivo não encontrado: {path}")
    cfg = json.loads(path.read_text())
    for key in ("participante", "repositorio"):
        if not cfg.get(key):
            sys.exit(f"participante.json precisa de '{key}'")
    return cfg


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


def docker_build(tag: str) -> None:
    run(["docker", "build", "-t", tag, str(REPO_ROOT)])


def docker_run(tag: str, participante: str, data_dir: str, network: str) -> int:
    pg_table = f"{participante}_empresas"
    container = f"test_{participante}"

    env = {
        "PARTICIPANTE": participante,
        "PG_TABLE": pg_table,
        "PG_HOST": os.getenv("PG_HOST", "postgres_db"),
        "PG_PORT": os.getenv("PG_PORT", "5432"),
        "PG_USER": os.getenv("PG_USER", "homelab_postgres"),
        "PG_PASSWORD": os.environ["PG_PASSWORD"],
        "PG_DB": os.getenv("PG_DB", "db_empresas"),
        "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minio_password"),
        "MINIO_BUCKET": os.getenv("MINIO_BUCKET", "marketing-leads"),
        "POLARS_SKIP_CPU_CHECK": "1",
    }

    data_path = Path(data_dir).resolve()
    if not data_path.is_dir():
        sys.exit(f"Pasta de dados inválida: {data_path}")
    zips = list(data_path.glob("*.zip"))
    print(f"\nZips em {data_path}: {len(zips)}")
    for z in sorted(zips):
        print(f"  - {z.name}")

    subprocess.run(["docker", "rm", "-f", container], capture_output=True)

    cmd = [
        "docker", "run", "--rm",
        "--name", container,
        "--cpus", "2.0",
        "--memory", "2g",
        "--memory-swap", "2g",
        "--network", network,
        "-v", f"{data_path}:/data:ro",
    ]
    for k, v in env.items():
        cmd += ["-e", f"{k}={v}"]
    cmd.append(tag)

    start = time.perf_counter()
    proc = subprocess.run(cmd)
    elapsed = time.perf_counter() - start
    print(f"\nTempo: {elapsed:.1f}s | exit code: {proc.returncode}")
    return proc.returncode


def check_postgres(participante: str) -> None:
    try:
        import psycopg2
    except ImportError:
        print("Instale psycopg2-binary para --check-db: pip install psycopg2-binary")
        return

    table = f"{participante}_empresas"
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST_CHECK", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        user=os.getenv("PG_USER", "homelab_postgres"),
        password=os.environ["PG_PASSWORD"],
        dbname=os.getenv("PG_DB", "db_empresas"),
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        )
        exists = cur.fetchone()[0] == 1
        print(f"\nTabela public.{table}: {'existe' if exists else 'NÃO existe'}")
        if exists:
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            print(f"Linhas: {cur.fetchone()[0]:,}")
    conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Teste local do pipeline Docker")
    parser.add_argument(
        "--data-dir", required=True,
        help="Pasta no host com os arquivos .zip (montada em /data:ro)",
    )
    parser.add_argument(
        "--network", default=os.getenv("DOCKER_NETWORK", "homelab_net"),
        help="Rede Docker onde postgres_db e minio estão",
    )
    parser.add_argument(
        "--check-db", action="store_true",
        help="Após o run, verifica tabela no Postgres (host localhost)",
    )
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    if "PG_PASSWORD" not in os.environ:
        sys.exit("Defina PG_PASSWORD no ambiente antes de rodar.")

    cfg = load_config()
    participante = cfg["participante"]
    tag = f"submissao_{participante}"

    print("=== Teste local — Ingestão no Limite ===")
    print(f"Repo        : {REPO_ROOT}")
    print(f"Participante: {participante}")
    print(f"Tabela      : public.{participante}_empresas")
    print(f"Rede Docker : {args.network}")

    if not args.skip_build:
        docker_build(tag)

    code = docker_run(tag, participante, args.data_dir, args.network)

    if args.check_db and code == 0:
        check_postgres(participante)

    if code == 0:
        print("\nOK — pipeline terminou com sucesso.")
    else:
        print(f"\nFALHOU — exit code {code} (starter ainda sai com 1 até implementar).")

    return code


if __name__ == "__main__":
    raise SystemExit(main())
