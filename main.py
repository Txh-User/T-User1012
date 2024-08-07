import time
import online_aggregation
import public_fuction as pf

def main() -> None:
    test_data = pf.data_load('./data/dataset.csv')

    # save_pkl('./data/dataset.pkl', test_data)
    # test_data = pf.load_pkl('./data/dataset.pkl')

    rules_list = pf.generate_rules('./data/rules.csv')
    # rules_list = pf.load_pkl('./data/rules.pkl')

    delta = 10
    online_aggregation.main(test_data, rules_list, delta)

if __name__ == '__main__':
    st_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    print(f'Run time: {end_time - st_time}s')