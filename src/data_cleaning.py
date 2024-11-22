import pandas as pd
import json

# Extraction des données
def load_data():
    drugs = pd.read_csv('data/drugs.csv')
    pubmed_csv = pd.read_csv('data/pubmed.csv')
    with open('data/pubmed.json', 'r') as f:
        pubmed_json = pd.DataFrame(json.load(f))
    clinical_trials = pd.read_csv('data/clinical_trials.csv')
    return drugs, pubmed_csv, pubmed_json, clinical_trials

# Nettoyage des données
def clean_data(drugs, pubmed_csv, pubmed_json, clinical_trials):
    # Standardiser les noms des colonnes
    pubmed_csv.columns = pubmed_csv.columns.str.lower()
    pubmed_json.columns = pubmed_json.columns.str.lower()
    clinical_trials.columns = clinical_trials.columns.str.lower()

    # Traiter les dates et supprimer les valeurs manquantes
    for df in [pubmed_csv, pubmed_json, clinical_trials]:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df.dropna(subset=['date', 'title'], inplace=True)

    return drugs, pubmed_csv, pubmed_json, clinical_trials
