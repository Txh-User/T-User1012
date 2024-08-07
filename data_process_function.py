from tqdm import tqdm
from datetime import datetime

def json2txt(input_data):
    result = {}

    for row in tqdm(input_data, desc='json2txt'):
        sid = row[0]
        bid = row[1]
        lf = row[2]
        date_object = datetime.fromtimestamp(int(row[5]))
        date_string = date_object.strftime('%Y-%m-%d %H:%M:%S')
        ctime = date_string[11:]
        day = date_string[:10]
        state = row[4]

        positions = (sid, bid, lf)

        if day not in result:
            result[day] = {}

        if positions not in result[day]:
            result[day][positions] = []

        result[day][positions].append((ctime, status_jduge_v0(state)))

    # pf.save_pkl('../data/json2txt.pkl', result)
    return result

def txt2time_series(txt_data):
    total_seconds_in_a_day = 24 * 60 * 60
    min_date = datetime.strptime('00:00:00', '%H:%M:%S')
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

                now_date = datetime.strptime(now_date, '%H:%M:%S')
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
    split_time = datetime.strptime('2023-03-11 12:00:00', '%Y-%m-%d %H:%M:%S')
    test_data = [i for i in data if datetime.strptime(i[4], '%Y-%m-%d %H:%M:%S') >= split_time]
    return test_data

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