import pickle
import csv
from scipy import stats

def save_pkl(filename, data):
    with open(filename, 'wb') as file:
        pickle.dump(data, file)

def load_pkl(filename):
    with open(filename, 'rb') as f:
        data = pickle.load(f)
    return data

def data_load(csv_file):
    print('data loading...')
    with open(csv_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        data = list(reader)

    return data

def generate_rules(rule_file_path):
    edge_set = set()

    with open(rule_file_path, 'r') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        for line in reader:
            node_pair = []
            nodes_str = line[2]
            nodes_str = nodes_str.split("=>")

            for node in nodes_str:
                node_pair.append(node)

            edge_set.add(tuple(node_pair))

    save_pkl('../data/rules.pkl', edge_set)
    return edge_set

def t_test(seq_1, seq_2):
    if len(seq_1) == 0 or len(seq_2) == 0:
        return True

    t_statistic, p_value = stats.ttest_ind(seq_1, seq_2)

    # print("t-statistic:", t_statistic)
    # print("p-value:", p_value)

    alpha = 0.05
    return p_value < alpha