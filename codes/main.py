import time
import online_aggregation
import public_fuction as pf

def main():
    test_data = pf.data_load('./data/dataset.csv')

    # save_pkl('./data/dataset.pkl', test_data)
    # test_data = pf.load_pkl('./data/dataset.pkl')

    rules_list = pf.generate_rules('./data/rules.csv')
    # rules_list = pf.load_pkl('./data/rules.pkl')

    online_aggregation.main(test_data, rules_list, delta1=10)

if __name__ == "__main__":
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter()
    print("Run time: {:.4f}s".format(end_time - start_time))
