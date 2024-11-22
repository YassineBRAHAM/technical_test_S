# technical_test_S

## I) Python et Data Engineering
### SOLUTION 1:
#### Structure du Projet:
    technical_test_S/
    ├── data/                   # Contient les fichiers de données
    │   ├── clinical_trials.csv
    │   ├── drugs.csv
    │   ├── pubmed.csv
    │   └── pubmed.json
    ├── output/                 # Contient les résultats
    │   └── result.json
    ├── dags/                    # Contient le code du dag
    │   └── drug_mentions_pipeline.py
    ├── tests/                  # Contient les tests unitaires
    └── requirements.txt        # Dépendances Python

#### Étapes de la Pipeline:
1. Installation de Airflow pour tester en local:
    - Pour installer et configurer Airflow :
    `pip install apache-airflow
    export AIRFLOW_HOME=~/airflow
    airflow db init
    airflow standalone`
    - Créez un utilisateur admin pour accéder à l'interface :
    `airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com`
2. Structure d'un DAG Airflow
Voici une proposition de workflow Airflow pour orchestrer le pipeline :

* Extraction des Données:
   - Charger les fichiers CSV et JSON en DataFrames pandas.
* Nettoyage des Données
   - Supprimer ou traiter les lignes avec des valeurs manquantes.
   - Standardiser les titres (par exemple, suppression de la casse et des espaces inutiles).
   - Vérifier les formats des dates et les convertir en un format cohérent.
* Traitement des Données
   - Identifier les mentions de médicaments dans les titres des articles et des essais cliniques.
   - Relier les médicaments à leurs journaux respectifs et inclure les dates des mentions.
* Génération du Graphe
   - Construire une structure JSON qui représente les relations entre les médicaments, les journaux, et les publications.
   - Utiliser des structures de type dictionnaire imbriqué pour gérer les relations et les métadonnées.
* Exportation des Résultats
   - Sauvegarder le graphe généré dans un fichier JSON dans le dossier output.
3. Gestion des déploiements de l'applications liée aux pipelines d’ingestion via Jenkins, 
des containers Docker, Google Kubernetes Engine (GKE) et 

### SOLUTION 2:
#### Structure du Projet:
Voici une structure suggérée pour le projet :

    technical_test_S/
    ├── data/                   # Contient les fichiers de données
    │   ├── clinical_trials.csv
    │   ├── drugs.csv
    │   ├── pubmed.csv
    │   └── pubmed.json
    ├── output/                 # Contient les résultats
    │   └── result.json
    ├── src/                    # Contient le code source
    │   ├── __init__.py
    │   ├── main.py             # Point d'entrée principal
    │   ├── data_cleaning.py    # Module pour le nettoyage des données
    │   ├── data_processing.py  # Module pour le traitement des données
    │   ├── data_extraction.py  # Module pour l'extraction de données
    │   └── graph_generator.py  # Module pour générer le graphe JSON
    ├── tests/                  # Contient les tests unitaires
    └── requirements.txt        # Dépendances Python

#### Étapes de la Pipeline:

1. Extraction des Données:
   - Charger les fichiers CSV et JSON en DataFrames pandas.
2. Nettoyage des Données
   - Supprimer ou traiter les lignes avec des valeurs manquantes.
   - Standardiser les titres (par exemple, suppression de la casse et des espaces inutiles).
   - Vérifier les formats des dates et les convertir en un format cohérent.
3. Traitement des Données
   - Identifier les mentions de médicaments dans les titres des articles et des essais cliniques.
   - Relier les médicaments à leurs journaux respectifs et inclure les dates des mentions.
4. Génération du Graphe
   - Construire une structure JSON qui représente les relations entre les médicaments, les journaux, et les publications.
   - Utiliser des structures de type dictionnaire imbriqué pour gérer les relations et les métadonnées.
5. Exportation des Résultats
   - Sauvegarder le graphe généré dans un fichier JSON dans le dossier output.


#### Résultat attendu:
Le fichier result.json aura une structure comme suit :

    `{
        "Aspirin": {
            "Journal A": ["2024-01-01", "2024-02-01"],
            "Journal B": ["2024-01-15"]
        },
        "Paracetamol": {
            "Journal C": ["2024-01-20"]
        }
    }`

#### Traitement Ad-hoc (Bonus):

##### Fonction pour extraire le journal qui mentionne le plus de médicaments différents
Cette fonction analysera les données du graphe JSON produit pour compter le nombre de médicaments différents mentionnés par chaque journal.

Code pour cette fonction :

    `def journal_with_most_drugs(graph):
        journal_counts = {}
        for drug, journals in graph.items():
            for journal in journals.keys():
                journal_counts[journal] = journal_counts.get(journal, set())
                journal_counts[journal].add(drug)
        
        # Convert the sets to counts
        journal_counts = {journal: len(drugs) for journal, drugs in journal_counts.items()}
        # Find the journal with the highest count
        max_journal = max(journal_counts, key=journal_counts.get)
        
        return max_journal, journal_counts[max_journal]`

##### Fonction pour trouver les médicaments mentionnés par les mêmes journaux que ceux d’un médicament donné (PubMed seulement)
Cette fonction identifiera, pour un médicament donné, tous les autres médicaments mentionnés dans les mêmes journaux, mais uniquement via des publications scientifiques (PubMed), et exclura les essais cliniques.

Code pour cette fonction :

    `def related_drugs_from_pubmed_only(graph, target_drug):
        related_drugs = set()
        pubmed_journals = set(graph.get(target_drug, {}).keys())
        
        for drug, journals in graph.items():
            if drug == target_drug:
                continue
            # Check overlap of journals between target drug and the current drug
            common_journals = pubmed_journals.intersection(journals.keys())
            if common_journals:
                related_drugs.add(drug)
        
        return related_drugs`

#### Réflexion sur les gros volumes de données
##### Problématiques avec de grosses volumétries :
1. Mémoire : Les fichiers volumineux peuvent dépasser la mémoire disponible.
2. Temps de traitement : Le traitement peut devenir très long.
3. Stockage : Des données volumineuses nécessitent des systèmes de fichiers distribués.
4. Orchestration : La gestion des dépendances entre les tâches devient cruciale.
##### Solutions possibles :
1. Traitement distribué :
   - Utilisez des frameworks comme Apache Spark pour le traitement distribué des données.
   - Ces outils permettent de traiter des données volumineuses en mémoire distribuée.
2. Stockage distribué :
   - Stockez les données sur des systèmes distribués comme HDFS ou des solutions cloud (GCS, S3, Azure Blob Storage).
3. Streaming de données :
   - Adoptez une approche de streaming avec des outils comme Apache Kafka ou Apache Flink pour traiter les données par blocs.
4. Base de données optimisée :
   - Utilisez une base de données spécialisée pour gérer des données volumineuses (e.g., Elasticsearch pour la recherche, Redshift pour l'analyse).
5. Optimisation du pipeline :
   - Partitionner les données en petits blocs.
   - Filtrer ou échantillonner les données pour les étapes exploratoires.

#### Modifications suggérées :
1. Gestion des E/S : Lisez et écrivez les fichiers en parquet ou ORC, qui sont des formats optimisés pour les gros volumes.
2. Orchestration : Déployez un orchestrateur comme Airflow pour automatiser et paralléliser les tâches.
----------------------------------------

## I) SQL:

* Première partie du test:

`SELECT SUM(prod_price * prod_qty)
FROM TRANSACTIONS as tr
Left JOIN PRODUCT_NOMENCLATURE AS pn
ON tr.prod_id = pn.product_id
WHERE date BETWEEN '01/01/2019' and '31/12/2019'
	AND 
GROUP BY client_id, product_type;`

---------------------------------
* Seconde partie du test:

`SELECT 
    tr.client_id,
    SUM(CASE WHEN pn.product_type = 'MEUBLE' THEN tr.prod_price * tr.prod_qty ELSE 0 END) AS ventes_meubles,
    SUM(CASE WHEN pn.product_type = 'DECO' THEN tr.prod_price * tr.prod_qty ELSE 0 END) AS ventes_deco
FROM 
    TRANSACTIONS tr
JOIN 
    PRODUCT_NOMENCLATURE pn
ON 
    tr.prop_id = pn.product_id
WHERE 
    tr.date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY 
    tr.client_id
ORDER BY 
    tr.client_id;`