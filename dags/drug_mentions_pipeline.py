from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import json

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    dag_id='drug_mentions_pipeline',
    default_args=default_args,
    description='Pipeline for processing drug mentions in publications',
    schedule_interval=None,  # Pas de planification automatique
    start_date=datetime(2024, 11, 21),
    catchup=False,
)


# Fonctions pour les tâches
def extract_data(**kwargs):
    # Charger les fichiers
    clinical_trials = pd.read_csv('/mnt/data/clinical_trials.csv')
    drugs = pd.read_csv('/mnt/data/drugs.csv')
    pubmed_csv = pd.read_csv('/mnt/data/pubmed.csv')
    with open('/mnt/data/pubmed.json', 'r') as f:
        pubmed_json = pd.DataFrame(json.load(f))

    # Ajouter les données dans le contexte Airflow (xcom)
    kwargs['ti'].xcom_push(key='clinical_trials', value=clinical_trials.to_dict())
    kwargs['ti'].xcom_push(key='drugs', value=drugs.to_dict())
    kwargs['ti'].xcom_push(key='pubmed_csv', value=pubmed_csv.to_dict())
    kwargs['ti'].xcom_push(key='pubmed_json', value=pubmed_json.to_dict())


def clean_data(**kwargs):
    # Extraire les données
    ti = kwargs['ti']
    clinical_trials = pd.DataFrame(ti.xcom_pull(key='clinical_trials'))
    drugs = pd.DataFrame(ti.xcom_pull(key='drugs'))
    pubmed_csv = pd.DataFrame(ti.xcom_pull(key='pubmed_csv'))
    pubmed_json = pd.DataFrame(ti.xcom_pull(key='pubmed_json'))

    # Renommer les colonnes pour uniformiser
    clinical_trials.rename(columns={'scientific_title': 'title'}, inplace=True)

    # Standardiser les colonnes
    def standardize_columns(df):
        df.columns = df.columns.str.lower().str.strip()
        return df

    clinical_trials = standardize_columns(clinical_trials)
    drugs = standardize_columns(drugs)
    pubmed_csv = standardize_columns(pubmed_csv)
    pubmed_json = standardize_columns(pubmed_json)

    # Re-pousser les données nettoyées
    ti.xcom_push(key='clinical_trials', value=clinical_trials.to_dict())
    ti.xcom_push(key='drugs', value=drugs.to_dict())
    ti.xcom_push(key='pubmed_csv', value=pubmed_csv.to_dict())
    ti.xcom_push(key='pubmed_json', value=pubmed_json.to_dict())


def process_mentions(**kwargs):
    # Extraire les données nettoyées
    ti = kwargs['ti']
    clinical_trials = pd.DataFrame(ti.xcom_pull(key='clinical_trials'))
    drugs = pd.DataFrame(ti.xcom_pull(key='drugs'))
    pubmed_csv = pd.DataFrame(ti.xcom_pull(key='pubmed_csv'))
    pubmed_json = pd.DataFrame(ti.xcom_pull(key='pubmed_json'))

    datasets = {
        'pubmed_csv': pubmed_csv,
        'pubmed_json': pubmed_json,
        'clinical_trials': clinical_trials,
    }

    # Identifier les mentions de médicaments
    def find_mentions(drug_list, datasets):
        mentions = []
        for drug in drug_list:
            for dataset_name, df in datasets.items():
                df_mentions = df[df['title'].str.contains(drug, case=False, na=False)].copy()
                for _, row in df_mentions.iterrows():
                    mentions.append({
                        'drug': drug,
                        'journal': row['journal'],
                        'date': row['date'],
                        'source': dataset_name,
                    })
        return pd.DataFrame(mentions)

    mentions = find_mentions(drugs['drug'], datasets)
    ti.xcom_push(key='mentions', value=mentions.to_dict())


def generate_graph(**kwargs):
    ti = kwargs['ti']
    mentions = pd.DataFrame(ti.xcom_pull(key='mentions'))

    # Construire le graphe JSON
    def generate_graph_from_mentions(mentions):
        graph = {}
        for _, row in mentions.iterrows():
            drug = row['drug']
            journal = row['journal']
            date = row['date']
            if drug not in graph:
                graph[drug] = {}
            if journal not in graph[drug]:
                graph[drug][journal] = []
            graph[drug][journal].append(date)
        return graph

    graph = generate_graph_from_mentions(mentions)

    # Sauvegarder le graphe
    with open('/mnt/data/drug_mentions_graph.json', 'w') as f:
        json.dump(graph, f, indent=4)


# Définir les tâches Airflow
t1 = PythonOperator(task_id='extract_data', python_callable=extract_data, provide_context=True, dag=dag)
t2 = PythonOperator(task_id='clean_data', python_callable=clean_data, provide_context=True, dag=dag)
t3 = PythonOperator(task_id='process_mentions', python_callable=process_mentions, provide_context=True, dag=dag)
t4 = PythonOperator(task_id='generate_graph', python_callable=generate_graph, provide_context=True, dag=dag)

# Définir le flux des tâches
t1 >> t2 >> t3 >> t4
