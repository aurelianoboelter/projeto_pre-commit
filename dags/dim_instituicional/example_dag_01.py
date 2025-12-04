from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="exemplo_taskflow",
    start_date=datetime(2025, 10, 13),
    schedule=None,
    catchup=False,
    tags=["exemplo", "dim_institucional", "teste"],
)
def exemplo_taskflow_dag():

    @task()
    def extrair_dados():
        return {"dados": [1, 2, 3]}

    @task()
    def transformar_dados(entrada):
        return [x * 2 for x in entrada["dados"]]

    @task()
    def carregar_dados(dados_transformados):
        print(f"Carregando dados! : {dados_transformados}")

    # Definindo dependências automaticamente
    dados = extrair_dados()
    transformados = transformar_dados(dados)
    carregar_dados(transformados)


exemplo_taskflow_dag = exemplo_taskflow_dag()
