from __future__ import annotations
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
def load_participante(repo: Path) -> dict:
    for name in ("participante.json", "participante.json.example"):
        p = repo / name
        if p.exists():
            return json.loads(p.read_text())
    raise FileNotFoundError(f"participante.json not found in {repo}")
def write_submission(official_repo: Path, participante: str, repo_url: str) -> Path:
    out = official_repo / "submissions" / f"{participante}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"participante": participante, "repositorio": repo_url}
    out.write_text(json.dumps(payload, indent=2) + "\n")
    return out
def docker_build(repo: Path, tag: str) -> None:
    subprocess.run(["docker", "build", "-t", tag, str(repo)], check=True)
def docker_run(tag: str, cfg: dict, data_dir: str, network: str) -> None:
  participante = cfg["participante"]
  pg_table = f"{participante}_empresas"
  env = {
      "PARTICIPANTE": participante,
      "PG_TABLE": pg_table,
      "PG_HOST": os.getenv("PG_HOST", "postgres_db"),
      "PG_PORT": os.getenv("PG_PORT", "5432"),
      "PG_USER": os.environ["PG_USER"],
      "PG_PASSWORD": os.environ["PG_PASSWORD"],
      "PG_DB": os.getenv("PG_DB", "db_empresas"),
      "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "http://minio:9000"),
      "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
      "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minio_password"),
      "MINIO_BUCKET": os.getenv("MINIO_BUCKET", "marketing-leads"),
      "POLARS_SKIP_CPU_CHECK": "1",
  }
  cmd = [
      "docker", "run", "--rm",
      "--name", f"app_test_{participante}",
      "--cpus", "2.0",
      "--memory", "2g",
      "--memory-swap", "2g",
      "--network", network,
      "-v", f"{data_dir}:/data:ro",
  ]
  for k, v in env.items():
      cmd += ["-e", f"{k}={v}"]
  cmd.append(tag)
  subprocess.run(cmd, check=True)
def run_evaluator(official_repo: Path, json_path: Path) -> None:
    evaluator = official_repo / "evaluator" / "evaluator.sh"
    subprocess.run([str(evaluator), str(json_path)], cwd=official_repo, check=True)
def cmd_test(args: argparse.Namespace) -> int:
    repo = Path(args.repo).resolve()
    cfg = load_participante(repo)
    tag = f"submissao_{cfg['participante']}"
    print(f"==> Building {tag} from {repo}")
    docker_build(repo, tag)
    print("==> Running pipeline (2 CPU, 2 GB, /data mounted)")
    docker_run(tag, cfg, args.data_dir, args.network)
    print("==> Container finished with exit 0 — check Postgres table manually or run evaluate")
    return 0
def cmd_evaluate(args: argparse.Namespace) -> int:
    official = Path(args.official_repo).resolve()
    json_path = Path(args.json).resolve()
    run_evaluator(official, json_path)
    return 0
def cmd_submit(args: argparse.Namespace) -> int:
    official = Path(args.official_repo).resolve()
    out = write_submission(official, args.participante, args.repo_url)
    print(f"Wrote {out}")
    print("\nNext steps:")
    print(f"  cd {official}")
    print(f"  git add {out.relative_to(official)}")
    print('  git commit -m "submissão: {args.participante}"')
    print("  git push origin main")
    print("  gh pr create --repo mpraes/ingestao_no_limite --base main --head <your-fork>:main")
    return 0
def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("test", help="build + docker run locally")
    t.add_argument("--repo", required=True, help="path to YOUR solution repo (with Dockerfile)")
    t.add_argument("--data-dir", required=True, help="host path with .zip files")
    t.add_argument("--network", default=os.getenv("DOCKER_NETWORK", "homelab_net"))
    t.set_defaults(func=cmd_test)
    e = sub.add_parser("evaluate", help="run evaluator/evaluator.sh (full gates)")
    e.add_argument("--official-repo", default=".", help="path to fork of ingestao_no_limite")
    e.add_argument("--json", required=True, help="e.g. submissions/dataforma-hub.json")
    e.set_defaults(func=cmd_evaluate)
    s = sub.add_parser("submit", help="write submissions/*.json in official fork")
    s.add_argument("--official-repo", default=".")
    s.add_argument("--participante", required=True)
    s.add_argument("--repo-url", required=True)
    s.set_defaults(func=cmd_submit)
    args = p.parse_args()
    return args.func(args)
if __name__ == "__main__":
    sys.exit(main())
