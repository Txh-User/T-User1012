import csv
from tqdm import tqdm, trange
import time
import warnings
from datetime import datetime, timedelta
import public_fuction as pf
import data_process_function as data_pf
import copy
warnings.filterwarnings("ignore")

def data_preprocess(raw_data, delta):
    print('data pre-process...')
    txt_data = data_pf.json_to_txt(raw_data) # json -> txt

    processed_data = data_pf.txt_to_time_series(txt_data) # txt -> csv

    pf.save_pkl(f'./data/tsData{delta}.pkl', processed_data)
    
    return processed_data

def find_patterns_and_handle(data, delta):
    # an easy example for handle different patterns
    length = len(data)
    processed_data = data.copy()

    def handle_stable_pattern(data, start_index, end_index):
        return

    def handle_fake_issuing_pattern(data, start_index, end_index):
        data[start_index: end_index + 1] = [0] * (end_index - start_index + 1)
        return data

    def handle_fake_dismissing_pattern(data, start_index, end_index, end_state):
        data[start_index: end_index + 1] = [end_state] * (end_index - start_index + 1)
        return data

    def handle_up_wander_pattern(data, start_index, end_index):
        data[start_index: end_index + 1] = [0] * (end_index - start_index + 1)
        return data

    def handle_down_wander_pattern(data, start_index, end_index):
        data[start_index: end_index + 1] = [0] * (end_index - start_index + 1)
        return data

    def handle_up_jitter_pattern(data, start_index, end_index):
        return

    def handle_down_jitter_pattern(data, start_index, end_index):
        return

    i = 0
    ALit = 0
    AL_itFl = 0
    collectPatternWindow = []
    fluctuationFinish = False

    while i < length:
        current_value = processed_data[i]
        
        j = i + 1

        while j < length and processed_data[j] == current_value:
            j += 1

        i = j
        ALit = processed_data[j - 1]

        # window = processed_data[j: j + delta]
        while j < length and (sum(processed_data[j: j + delta]) / len(processed_data[j: j + delta]) != 1 and sum(processed_data[j: j + delta]) / len(processed_data[j: j + delta]) != 0):
            collectPatternWindow.append(processed_data[j])
            j += 1

        AL_itFl = processed_data[j] if j < length else 0
        if len(collectPatternWindow) != 0:
            fluctuationFinish = True

        if fluctuationFinish:
            if ALit == AL_itFl == 0:
                processed_data = handle_fake_issuing_pattern(processed_data, i, j - 1)
            elif ALit == AL_itFl != 0:
                processed_data = handle_fake_dismissing_pattern(processed_data, i, j - 1, AL_itFl)
            elif ALit == 0 and AL_itFl != 0:
                processed_data = handle_up_wander_pattern(processed_data, i, j - 1)
            elif  ALit != 0 and AL_itFl == 0:
                processed_data = handle_down_wander_pattern(processed_data, i, j - 1)

        i = j
        fluctuationFinish = False
        collectPatternWindow = []
        ALit = 0
        AL_itFl = 0

    return processed_data

def sensorTierAgg(test_data, delta):
    # print("sensorTier process start")
    sensorTierProcessedData = {}
    txt_data = {}
    ob_agg_data = []

    for day, t in tqdm(test_data.items(), desc='sensorTier1'):
        if day not in sensorTierProcessedData:
            sensorTierProcessedData[day] = []

        for row in t:
            processed_data = []
            pos = row[0]
            processed_data.append(pos)
            patternProcessedData = find_patterns_and_handle(row[1:], delta)
            processed_data[1:] = patternProcessedData
            sensorTierProcessedData[day].append(processed_data)
    
    for day, t in tqdm(sensorTierProcessedData.items(), desc='sensorTier2'):
        if day not in txt_data:
            txt_data[day] = {}

        for row in t:
            position = row[0]
            if position not in txt_data:
                txt_data[day][position] = []

            for index, state in enumerate(row):
                if index == 0: continue
                preState = row[index - 1]
                if state != preState and preState != position:
                    current_time = time.strftime('%H:%M:%S', time.gmtime(index))
                    current_status = state
                    txt_data[day][position].append((current_time, current_status))

    for day, t in txt_data.items():
        for key, value in t.items():
            sid = key[0]
            bid = key[1]
            lf = key[2]
            for index, data in enumerate(value):
                timestamp = day + ' ' + data[0]
                state_code = data[1]
                if len(value) == 1:
                    if state_code == 0:
                        state = 'dnc'
                    else:
                        state = data_pf.status_jduge_v1(state_code)
                else:
                    if state_code == 0 and index != 0:
                        state = data_pf.status_jduge_v2(value[index-1][1], value[index][1])
                    elif state_code == 0 and index == 0:
                        state = data_pf.status_jduge_v2(value[index][1], value[index+1][1])
                    else:
                        state = data_pf.status_jduge_v1(state_code)

                ob_agg_data.append([sid, bid, lf, state, timestamp])

    ob_agg_data = sorted(ob_agg_data, key = lambda x:x[4])

    # pf.save_pkl(f'./data/sensorTierProcessed{delta}.pkl', ob_agg_data)
    # ob_agg_data = pf.load_pkl(f'./data/sensorTierProcessed{delta}.pkl')

    return ob_agg_data

def systemTierAgg(ob_agg_data, rules_list, delta, window_clock_size):
    # print("systemTier process start")

    window_size = timedelta(seconds=window_clock_size)
    _, test_data = data_pf.split_dataset(ob_agg_data)
    result_set = set(map(tuple, test_data))

    for index in trange(len(test_data), desc='systemTier'):
        row = test_data[index]
        
        column1_data = (row[2], row[1], row[0])
        
        timestamp1 = datetime.strptime(row[4], '%Y-%m-%d %H:%M:%S')
        endtime = timestamp1 + window_size

        for idx in range(index + 1, len(test_data)):
            rows = test_data[idx]
            column2_data = (rows[2], rows[1], rows[0])
            
            if column2_data == column1_data: # same alert
                continue

            timestamp2 = datetime.strptime(rows[4], '%Y-%m-%d %H:%M:%S')

            if timestamp2 > endtime:
                break
            
            compare = (column1_data, column2_data)
            compare_str = tuple(map(str, compare))
            if compare_str in rules_list and tuple(rows) in result_set:
                result_set.remove(tuple(rows))

    result = list(result_set)
    result = sorted(result, key = lambda x:x[4])

    # pf.save_pkl(f'./data/systemTierProcessedData{delta}.pkl', result)
    # result = pf.load_pkl(f'./data/systemTierProcessedData{delta}.pkl')
    
    evaluation(result)
    return result

def evaluation(eval_data):
    aggLen = 528954
    true_file = './data/groundtruth.csv'
    split_time = '2023-05-20 05:04:11'

    y_true = []
    with open(true_file, 'r', newline="") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        for row in reader:
            timestamp = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S")

            if timestamp >= datetime.strptime(split_time, "%Y-%m-%d %H:%M:%S"):
                y_true.append(row)

    
    y_pred = eval_data
    num = 0
    for i in y_true:
        tp = tuple(i)
        if tp in y_pred:
            num += 1

    agg_rate = (1 - (len(eval_data) / aggLen)) * 100
    acc = (num / len(y_true)) * 100

    print("before: {}, after: {}, aggregation rate: {:.2f}%".format(aggLen, len(eval_data), agg_rate))
    print("true: {}, pred: {}, accuracy: {:.2f}%".format(len(y_true), num, acc))

def data_save(processed_data, save_file):
    header = ['sid','bid','lf','state','timestamp']
    with open(save_file, 'w', newline="") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(processed_data)

def main(raw_data, rules_list, delta):
    print('delta: {}'.format(delta))

    test_data = data_preprocess(raw_data, delta)

    # test_data = pf.load_pkl('./data/tsData{}.pkl'.format(delta))

    starttime1 = time.perf_counter()
    sensorTierProcessedDate = sensorTierAgg(test_data, delta)
    endtime1 = time.perf_counter()
    print("sensorTierTime: {:.4f}s".format(endtime1 - starttime1))

    sensorTier_data_save_path = f'./data/sensorTierDelta{delta}.csv'
    data_save(sensorTierProcessedDate, sensorTier_data_save_path)

    window_clock_size = 600
    starttime2 = time.perf_counter()
    systemTierProcessedDate = systemTierAgg(sensorTierProcessedDate, rules_list, delta, window_clock_size)
    endtime2 = time.perf_counter()
    print("systemTierTime: {:.4f}s".format(endtime2 - starttime2))

    systemTier_data_save_path = f'./data/systemTierDelta{delta}.csv'
    data_save(systemTierProcessedDate, systemTier_data_save_path)

if __name__ == '__main__':
    st_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    print(f'Run time: {end_time - st_time}s')
