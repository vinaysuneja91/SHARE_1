from input_module import read_from_mysql
from output_module import write_to_mysql
import pandas as pd
import requests

# option_key_data = read_from_mysql("select * from V_OPTION_KEY_CE")
# option_key_data_tup = option_key_data[0]

# print(option_key_data)
# print(option_key_data_tup)


def get_margin_data(option_key_data):
    output_df = pd.DataFrame()
    count = 0
    err_count = 0
    total_runs = len(option_key_data)

    pct_completed = 0

    print("Total Expected Runs", total_runs)

    for option_key_data_tup in option_key_data:
        data = {
            'action': 'calculate',
            'exchange[]': 'NFO',
            'product[]': 'OPT',
            'scrip[]': option_key_data_tup[0],  # 'BANDHANBNK21APR',
            'option_type[]': option_key_data_tup[4],
            'strike_price[]': option_key_data_tup[1],
            'qty[]': option_key_data_tup[2],
            'trade[]': 'sell'
        }

        # data = {'action': 'calculate', 'exchange[]': 'NFO', 'product[]': 'OPT', 'scrip[]': 'RELIANCE21APR',
        #         'option_type[]': 'CE', 'strike_price[]': '1360', 'qty[]': '250', 'trade[]': 'sell'}

    #    print("Runnig for - ")
    #    print(data)
        # print(option_key_data[0])
        # print(type(option_key_data[0]))
        # print(res)

        response = requests.post(
            'https://zerodha.com/margin-calculator/SPAN',  data=data)
        # print(response.text)
        return_data = response.json()
        # print('Return response is-', response.json())
        # print(type(response.json()))

        if return_data['last'] != []:

            # print(return_data['last']['total'])
            total_amt = return_data['last']['total']
            # print("Total Amt is", total_amt)

            # print(type(total_amt))
            option_key = option_key_data_tup[3]

            # print(option_key_data_tup[3])

            data_temp = [{'OPTION_KEY': option_key, 'TOTAL_MARGIN_AMT': total_amt}
                         ]

            # Creates DataFrame.
            # df_temp = pd.DataFrame(data_temp)

            # success count
            count = count + 1
            output_df = output_df.append(data_temp)
        else:
            err_count = err_count + 1

    #       print(df_temp)

        pct_completed = round((count * 100) / total_runs, 2)
        print('Runs Completed - ', count)
        print('Error Counts - ', err_count)
        print('% Completed - ', pct_completed, '%')

    print(output_df)
    # print(type(output_df))
    return output_df
