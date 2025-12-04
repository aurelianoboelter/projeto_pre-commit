"""
Apenas um teste basico para execução com pytest no pre-commit.
Pensando em Validar se o ambiente Python e as dependências estão funcionando corretamente.
"""

import sys

import pytest


def test_python_version():
    """Aqui vou verificar se a versão do Python é compatível com o projeto."""
    # assert sys.version_info <= (2, 7), "Versão mínima do Python requerida: 2.7"
    assert sys.version_info >= (3, 10), "Versão mínima do Python requerida: 3.10"


def test_airflow_import():
    """Valida se o Airflow pode ser importado corretamente."""
    try:
        import airflow  # noqa: F401
    except ImportError:
        pytest.fail("Apache Airflow não está instalado ou não pode ser importado.")


def test_dag_folder_exists(tmp_path_factory):
    """Verifica se o diretório 'dags/' existe (estrutura mínima do projeto)."""
    import os

    dags_path = os.path.join(os.getcwd(), "dags")
    assert os.path.exists(dags_path), "Diretório 'dags/' não encontrado."
