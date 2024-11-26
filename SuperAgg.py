import csv
from tqdm import tqdm, trange
import time
from datetime import datetime, timedelta
import public_fuction as pf
import data_process_function as data_pf

class SuperAgg:
    def __init__(self) -> None:
        self.delta = 10
        self.window_clock_size = 600

    def data_preprocess(self, raw_data):
        print('data pre-process...')
        txt_data = data_pf.json2txt(raw_data)
        processed_data = data_pf.txt2time_series(txt_data)

        pf.save_pkl(f'./data/tsData.pkl', processed_data)
        return processed_data

    def find_patterns_and_handle(self, data):
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

            while j < length and (sum(processed_data[j: j + self.delta]) / len(processed_data[j: j + self.delta]) != 1 and sum(processed_data[j: j + self.delta]) / len(processed_data[j: j + self.delta]) != 0):
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

    def sensorTierAgg(self, raw_data, test_data, delta):
        raw_data_dict = {(i[0], i[1], i[2], i[5]): i for i in raw_data}
        sensorTierProcessedData = {}
        txt_data = {}
        ob_agg_data = []

        # for day, t in tqdm(test_data.items(), desc='sensorTier1'):
        #     if day not in sensorTierProcessedData:
        #         sensorTierProcessedData[day] = []

        #     for row in t:
        #         processed_data = []
        #         pos = row[0]
        #         processed_data.append(pos)
        #         patternProcessedData = self.find_patterns_and_handle(row[1:])
        #         processed_data[1:] = patternProcessedData
        #         sensorTierProcessedData[day].append(processed_data)
        
        # for day, t in tqdm(sensorTierProcessedData.items(), desc='sensorTier2'):
        #     if day not in txt_data:
        #         txt_data[day] = {}

        #     for row in t:
        #         position = row[0]
        #         if position not in txt_data:
        #             txt_data[day][position] = []

        #         for index, state in enumerate(row[1:]):
        #             preState = row[index - 1]
        #             if state != preState and preState != position:
        #                 current_time = day + ' ' + time.strftime('%H:%M:%S', time.gmtime(index))
        #                 current_time_timestamp = str(int(datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S").timestamp()))
        #                 txt_data[day][position].append(current_time_timestamp)
        
        # pf.save_pkl(f'./data/sensor_txt_data.pkl', txt_data)
        # txt_data = pf.load_pkl(f'./data/sensor_txt_data.pkl')

        # for _, t in tqdm(txt_data.items(), desc='sensorTier3'):
        #     for key, value in t.items():
        #         for st in value:
        #             new_key = (*key, st)
        #             if new_key in raw_data_dict:
        #                 ob_agg_data.append(raw_data_dict[new_key])

                    # timestamp = day + ' ' + data[0]
                    # state_code = data[1]
                    # if len(value) == 1:
                    #     if state_code == 0:
                    #         state = 'dnc'
                    #     else:
                    #         state = data_pf.status_jduge_v1(state_code)
                    # else:
                    #     if state_code == 0 and index != 0:
                    #         state = data_pf.status_jduge_v2(value[index-1][1], value[index][1])
                    #     elif state_code == 0 and index == 0:
                    #         state = data_pf.status_jduge_v2(value[index][1], value[index+1][1])
                    #     else:
                    #         state = data_pf.status_jduge_v1(state_code)

                            

        # ob_agg_data = sorted(ob_agg_data, key = lambda x:x[5])

        # pf.save_pkl(f'./data/sensorTierProcessed.pkl', ob_agg_data)
        ob_agg_data = pf.load_pkl(f'./data/sensorTierProcessed.pkl')

        return ob_agg_data

    def systemTierAgg(self, ob_agg_data, rules_list, delta, window_clock_size, raw_data_len):
        test_data = data_pf.split_dataset(ob_agg_data)
        result_set = set(map(tuple, test_data))

        for index in trange(len(test_data), desc='systemTier'):
            row = test_data[index]
            
            column1_data = (row[2], row[1], row[0])
            
            timestamp1 = int(row[5])
            endtime = timestamp1 + window_clock_size

            for idx in range(index + 1, len(test_data)):
                rows = test_data[idx]
                column2_data = (rows[2], rows[1], rows[0])
                
                if column2_data == column1_data: # same alert
                    continue

                timestamp2 = int(rows[5])

                if timestamp2 > endtime:
                    break
                
                compare = (column1_data, column2_data)
                compare_str = tuple(map(str, compare))
                if compare_str in rules_list and tuple(rows) in result_set:
                    result_set.remove(tuple(rows))

        result = list(result_set)
        result = sorted(result, key = lambda x:x[5])

        # pf.save_pkl(f'./data/systemTierProcessedData{delta}.pkl', result)
        # result = pf.load_pkl(f'./data/systemTierProcessedData{delta}.pkl')
        
        self.evaluation(result, raw_data_len)
        return result

    def evaluation(self, eval_data, raw_data_len):
        true_file = './data/groundtruth.csv'
        split_timestamp = 1678507200

        y_true = []
        with open(true_file, 'r', newline='') as infile:
            reader = csv.reader(infile)
            header = next(reader)
            for row in reader:
                timestamp = int(row[4])

                if timestamp >= split_timestamp:
                    y_true.append(row)

        y_pred = [[i[0], i[1], i[2], i[4], i[5]] for i in eval_data]
        num = 0
        for i in y_true:
            true_position = [i[0], i[1], i[2], i[3]]
            true_ts = int(i[4])
            for j in y_pred:
                pred_position = [j[0], j[1], j[2], j[3]]
                pred_ts = int(j[4])
                if true_position == pred_position and abs(true_ts - pred_ts) <= 2:
                    num += 1

        agg_rate = (1 - (len(eval_data) / raw_data_len)) * 100
        acc = (num / len(y_true)) * 100

        print('before: {}, after: {}, aggregation rate: {:.2f}%'.format(raw_data_len, len(eval_data), agg_rate))
        print('true: {}, pred: {}, accuracy: {:.2f}%'.format(len(y_true), num, acc))

    def data_save(self, processed_data, save_file):
        header = ['sid','bid','lf','state','timestamp']
        with open(save_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            writer.writerows(processed_data)

    def run(self, raw_data, rules_list):
        print('delta: {}'.format(self.delta))
        raw_data_len = len(raw_data)

        # test_data = self.data_preprocess(raw_data)
        test_data = pf.load_pkl('./data/tsData.pkl')

        # starttime1 = time.perf_counter()
        sensorTierProcessedDate = self.sensorTierAgg(raw_data, test_data, self.delta)
        # endtime1 = time.perf_counter()
        # print('sensorTierTime: {:.4f}s'.format(endtime1 - starttime1))

        # starttime2 = time.perf_counter()
        _ = self.systemTierAgg(sensorTierProcessedDate, rules_list, self.delta, self.window_clock_size, raw_data_len)
        # endtime2 = time.perf_counter()
        # print('systemTierTime: {:.4f}s'.format(endtime2 - starttime2))