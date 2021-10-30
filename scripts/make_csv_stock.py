import os
import sys
import statistics

CSV_HEADER = 'System,Query,Window Length,Timeout,ExecTime,Events,EnumTime,Matches,Throughput,MAXTotal,AVGTotal,MAXUsed,AVGUsed\n'
ITERATIONS = 3
TEST_NAMES = ['stock2']#, 'seq_sel', 'seq_no_last_event']

def collect_data(timestamp):
    print(timestamp)
    time_data_dict = {}
    memory_data_dict = {}
    if not os.path.exists('./csv'):
        os.makedirs('./csv')
    for f in os.listdir(f'./results/{timestamp}/results'):
        if f.endswith('err.txt'):
            continue
        f_data = f.split('.')[0]
        system, _, query, window, test = f_data.split('_')
        if test == 'Time':
            data_dict = time_data_dict
        else:
            data_dict = memory_data_dict
        if f.endswith('except.txt'):
            with open(f'./results/{timestamp}/results/{f}', 'r') as tf:
                tf.readline()
                timeout = tf.readline().strip()
            data_dict[(system, query, window)] = [f'{timeout},-1,-1,-1,-1',
                                                            f'{timeout},-1,-1,-1,-1',
                                                            f'{timeout},-1,-1,-1,-1']
            continue
        with open(f'./results/{timestamp}/results/{f}') as tf:
            data = tf.readlines()
        data = data[1:]
        data = [d.strip() for d in data]
        if test == 'Time':
            newdata = []
            for d in data:
                temp = d.split(',')
                print(temp)
                temp.append(str(float(temp[2])/float(temp[1])))
                newdata.append(','.join(temp))
            data = newdata
        data_dict[(system, query, window)] = data
    merged_data_dict = time_data_dict
    # merged_data_dict = {}
    # for key in time_data_dict.keys():
    #     time_data = time_data_dict[key]
    #     if key in memory_data_dict:
    #         memory_data = memory_data_dict[key]
    #     else:
    #         memory_data = [f'-1,-1,-1,-1',
    #                         f'-1,-1,-1,-1',
    #                         f'-1,-1,-1,-1']
    #     merged_data = []
    #     for i in range(len(time_data)):
    #         merged_data.append(time_data[i] + ',' + memory_data[i])
    #         # merged_data.append(time_data[i])
    #     merged_data_dict[key] = merged_data
    
    with open(f'./csv/all_data_{timestamp}.csv', 'w') as tf:
        tf.write(f'{CSV_HEADER}')
        for k, v in merged_data_dict.items():
            system, events, window = k
            for d in v:
                tf.write(f'{system},{events},{int(window)},{d}\n')

    with open(f'./csv/avg_data_{timestamp}.csv', 'w') as tf:
        tf.write(f'{CSV_HEADER}')
        for k, v in merged_data_dict.items():
            system, events, window = k
            data_list = []
            for d in range(len(v)):
                data_list.append(v[d].split(','))
                if not (d + 1) % ITERATIONS: 
                    sum_data = [str(round(mysum(x)/3, 5)) for x in zip(*data_list)]
                    tf.write(f'{system},{events},{int(window)},{",".join(sum_data)}\n')
                    data_list = []

    with open(f'./csv/median_data_{timestamp}.csv', 'w') as tf:
        tf.write(f'{CSV_HEADER}')
        for k, v in merged_data_dict.items():
            system, events, window = k
            data_list = []
            for d in range(len(v)):
                data_list.append(v[d].split(','))
                if not (d + 1) % ITERATIONS: 
                    sum_data = [str(mymedian(x)) for x in zip(*data_list)]
                    tf.write(f'{system},{events},{int(window)},{",".join(sum_data)}\n')
                    data_list = []


def mymedian(args):
    data = []
    for i in args:
        data.append(float(i))
    return statistics.median(data)


def mysum(args):
    total = 0
    for i in args:
        total += float(i)
    return total

def main():
    for timestamp in TEST_NAMES:
        collect_data(timestamp)



if __name__ == '__main__':
    main()
