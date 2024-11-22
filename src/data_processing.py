def match_drugs_with_titles(drugs, dataframes):
    drug_mentions = []

    for drug in drugs['drug']:
        for df in dataframes:
            mentions = df[df['title'].str.contains(drug, case=False, na=False)]
            for _, row in mentions.iterrows():
                drug_mentions.append({
                    'drug': drug,
                    'journal': row['journal'],
                    'date': row['date'],
                    'source': row.get('id', 'N/A')
                })
    return pd.DataFrame(drug_mentions)
