import pandas as pd
import os

from constants import ResultsColumns, EdgesColumn, NodesColumn


def get_full_path(file_path: str):
    return os.path.join(os.path.dirname(__file__), file_path)


def read_file(file_name: str):
    print("Reading file:", file_name)
    file_full_path = os.path.dirname(__file__) + '/before-data/' + file_name + '.csv'
    return pd.read_csv(file_full_path)


def transform_nodes(data):
    print("Creating nodes")
    all_team_set = list(set(data[ResultsColumns.HOME_TEAM].append(data[ResultsColumns.AWAY_TEAM])))
    all_team_set.sort()

    nodes = pd.DataFrame(list(zip(all_team_set, all_team_set)), columns=[NodesColumn.ID, NodesColumn.NAME])
    nodes.to_csv('./after-data/nodes.csv', index=False)
    print("Created nodes excel file")


def extract_edges(data):
    print("Creating edges")
    temp_edges = {}

    for i in range(len(data)):
        current_home_team = data[ResultsColumns.HOME_TEAM][i]
        current_home_score = data[ResultsColumns.HOME_SCORE][i]
        current_away_team = data[ResultsColumns.AWAY_TEAM][i]
        current_away_score = data[ResultsColumns.AWAY_SCORE][i]

        if current_home_score == current_away_score:
            continue

        [winning_team, losing_team] = [current_home_team, current_away_team] \
            if current_home_score > current_away_score else [current_away_team, current_home_team]

        key = losing_team + '_' + winning_team

        if key in temp_edges.keys():
            temp_edges[key][EdgesColumn.WEIGHT] += 1
        else:
            temp_edges[key] = {EdgesColumn.WEIGHT: 1}
    return temp_edges


def transform_edges(mid_edges):
    edges = pd.DataFrame(columns=[EdgesColumn.SOURCE, EdgesColumn.TARGET, EdgesColumn.WEIGHT])

    i = 0

    for [key, value] in mid_edges.items():
        team_key = key.split('_')
        winning_team_key = team_key[0]
        losing_team_key = team_key[1]
        edges = edges.append({EdgesColumn.TARGET: winning_team_key, EdgesColumn.SOURCE: losing_team_key,
                              EdgesColumn.WEIGHT: value[EdgesColumn.WEIGHT]},
                             ignore_index=True)
        i += 1

    edges.to_csv('./after-data/edges.csv', index=False)
    print("Created edges excel file")


df = read_file('results')

df = df.drop(['date', 'tournament', 'city', 'country', 'neutral'], axis=1)

# Create nodes
transform_nodes(df)

# Create edges
temp_edges_data = extract_edges(df)

transform_edges(temp_edges_data)
