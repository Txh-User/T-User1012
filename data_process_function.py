import tqdm
from datetime import datetime

def json_to_txt(input_data):
    result = {}

    for row in tqdm(input_data, desc='json2txt'):
        sid = row[0]
        bid = row[1]
        lf = row[7]
        ctime = row[11][11:]
        day = row[11][:10]
        state = row[10]

        positions = (sid, bid, lf)

        if day not in result:
            result[day] = {}

        if positions not in result[day]:
            result[day][positions] = []

        result[day][positions].append((ctime, status_jduge_v0(state)))

    # pf.save_pkl('./data/json2txt.pkl', result)
    return result

def txt_to_time_series(txt_data):
    total_seconds_in_a_day = 24 * 60 * 60
    min_date = datetime.strptime("00:00:00", "%H:%M:%S")
    csv_data = {}

    for day, day_data in tqdm(txt_data.items(), desc='txt2ts'):
        data_line = 0
        if day not in csv_data:
            csv_data[day] = []

        csv_data[day] = [0] * len(day_data)
        for position, line in day_data.items():
            second = 0
            data = []
            data.append(position)
            for index, item in enumerate(line):
                now_date = item[0]
                if index != 0:
                    front_date = line[index - 1][0]
                else:
                    front_date = None
                now_value = item[1]

                if now_date == front_date:
                    continue

                if index != 0:
                    front_value = line[index - 1][1]
                else:
                    front_value = now_value

                now_date = datetime.strptime(now_date, "%H:%M:%S")
                gap = int((now_date - min_date).total_seconds())
                while(second < total_seconds_in_a_day):
                    if(second == gap):
                        data.append(now_value)
                        second += 1
                        break
                    else:
                        data.append(front_value)
                        second += 1

            # fill the list
            data = data + [now_value] * (total_seconds_in_a_day - len(data) + 1)

            csv_data[day][data_line] = data
            data_line += 1
    
    # pf.save_pkl('./data/txt2ts.pkl', csv_data)
    return csv_data

def split_dataset(data):
    split_ratio = 0.7
    train_len = len(data) * split_ratio
    train_data = data[:train_len]
    test_data = data[train_len:]
    return train_data, test_data

def status_jduge_v0(a):
    if(a[0] == 'd'):
        return 0
    elif(a == 'nc'):
        return 1
    elif(a == 'cr'):
        return 2
    elif(a == 'nr'):
        return 3

def status_jduge_v1(a):
    if(a == 1):
        return 'nc'
    elif(a == 2):
        return 'cr'
    elif(a == 3):
        return 'nr'
    
def status_jduge_v2(a, b):
    if (a == 0) and (b == 0):
        return 'dnc'
    
    if (a == 0):
        if(b == 1):
            return 'dnc'
        elif(b == 2):
            return 'dcr'
        elif(b == 3):
            return 'dnr'

    elif (b == 0):
        if(a == 1):
            return 'dnc'
        elif(a == 2):
            return 'dcr'
        elif(a == 3):
            return 'dnr'