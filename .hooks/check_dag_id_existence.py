#!/usr/bin/env python3
import re
import sys
from pathlib import Path

# Permite capturar dag_id mesmo com múltiplas linhas
DAG_ID_PATTERN = re.compile(r"dag_id\s*=\s*['\"]([^'\"]+)['\"]", re.DOTALL)


def extract_dag_ids(path: Path) -> list[str]:
    """Extrai todos os valores de 'dag_id' usando regex."""
    try:
        text = path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"[ERRO DE LEITURA] {path}: {e}", file=sys.stderr)
        return []
    return list(set(DAG_ID_PATTERN.findall(text)))


def find_all_dag_files(base_dir: Path) -> list[Path]:
    """Busca todos os arquivos Python dentro da pasta 'dags' (recursivamente)."""
    return [f for f in base_dir.rglob("*.py") if "dags" in str(f)]


def main(argv=None):
    """Itera sobre todos os ficheiros e verifica duplicatas de dag_id."""
    base_dir = Path(__file__).resolve().parents[1] / "dags"

    if not base_dir.exists():
        print(f"[AVISO] Pasta não encontrada: {base_dir}")
        return 0  # evita falhas desnecessárias

    found_dag_ids: dict[str, list[Path]] = {}

    # Se o pre-commit passou arquivos, usa-os; senão, varre todos
    if len(sys.argv) > 1:
        files_to_check = [Path(f) for f in sys.argv[1:] if f.endswith(".py")]
    else:
        files_to_check = find_all_dag_files(base_dir)

    if not files_to_check:
        print("[AVISO] Nenhum arquivo .py encontrado em dags/.")
        return 0

    for path in files_to_check:
        ids_in_file = extract_dag_ids(path)
        for dag_id in ids_in_file:
            found_dag_ids.setdefault(dag_id, []).append(path)

    has_duplicates = False

    for dag_id, paths in found_dag_ids.items():
        if len(paths) > 1:
            print("--------------------------------------------------")
            print(f"[ERRO FATAL] DAG ID DUPLICADO DETETADO: '{dag_id}'")
            print("Encontrado nos seguintes ficheiros:")
            for p in paths:
                print(f"  -> {p}")
            print("--------------------------------------------------")
            has_duplicates = True

    if not has_duplicates:
        print("✅ Todos os DAGs possuem IDs únicos e válidos.")

    return 1 if has_duplicates else 0


if __name__ == "__main__":
    sys.exit(main())
