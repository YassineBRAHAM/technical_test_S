def generate_graph(mentions):
    graph = {}
    for _, row in mentions.iterrows():
        drug = row['drug']
        journal = row['journal']
        date = row['date']

        if drug not in graph:
            graph[drug] = {}
        if journal not in graph[drug]:
            graph[drug][journal] = []
        graph[drug][journal].append(str(date))

    return graph
