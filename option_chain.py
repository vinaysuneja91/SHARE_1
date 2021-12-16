# import requests
from nsepython import *
from output_module import write_to_mysql
from input_module import read_from_mysql
from nse_tools_api import ltp_stock, fno_lot_sizes


# not working
# url = "http://maps.googleapis.com/maps/api/geocode/json?address=googleplex&sensor=false"
# url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
# "https://www.nseindia.com/api/option-chain-equities?symbol=BANDHANBNK"
# r = requests.get(url)
# print(r.json())
# not working


def get_option_chain_data(stocklist):
    df_ce_data = pd.DataFrame([])
    df_pe_data = pd.DataFrame([])
    df_oi_data = pd.DataFrame([])
    for stockname in stocklist:
        print("Running for",stockname)
        #        dict_data = nse_optionchain_scrapper('NIFTY')
        dict_data = nse_optionchain_scrapper(stockname)
        #print(dict_data)
        final_ce_data = pd.DataFrame([])
        final_pe_data = pd.DataFrame([])
        for item in dict_data['records']['data']:
            # print(item)
            # print(type(item))
            if 'CE' in item.keys():
                # print(item['CE'])
                # print(type(item['CE']))
                data_dict = item['CE']
                # print(data_dict)
                # print('###########')
                data_items = data_dict.items()
                data_list = list(data_items)
                # print(data_list)
                df = pd.DataFrame(data_list)
                # print(df)
                df1 = df.transpose()
                # print(df1)
                df1.columns = df1.iloc[0]
                df1.drop(df1.index[:1], inplace=True)
        #        df1.columns = df1[:1]
        #        df1 = df1[1:]
                # print(df1)
                # final_data = df1
                final_ce_data = final_ce_data.append(df1)
                # print(df1.columns)
            # print(final_data)

            if 'PE' in item.keys():
                # print(item['CE'])
                # print(type(item['CE']))
                data_dict = item['PE']
                # print(data_dict)
                # print('###########')
                data_items = data_dict.items()
                data_list = list(data_items)
                # print(data_list)
                df = pd.DataFrame(data_list)
                # print(df)
                df1 = df.transpose()
                # print(df1)
                df1.columns = df1.iloc[0]
                df1.drop(df1.index[:1], inplace=True)
        #        df1.columns = df1[:1]
        #        df1 = df1[1:]
                # print(df1)
                # final_data = df1
                final_pe_data = final_pe_data.append(df1)
                # print(df1.columns)
            # print(final_data)

        df_ce_data = df_ce_data.append(final_ce_data)
        df_pe_data = df_pe_data.append(final_pe_data)
    #    print(final_data)
        dict_ce_oi = dict_data['filtered']['CE']
        totOI_ce = dict_ce_oi['totOI']
        totVol_ce = dict_ce_oi['totVol']
        df_ce_oi = pd.DataFrame([[stockname,'CE',totOI_ce,totVol_ce]],columns = ['SCRIP','TYPE','TOTOI','TOTVOL'])
        #print(df_ce_oi)
        dict_pe_oi = dict_data['filtered']['PE']
        totOI_pe = dict_pe_oi['totOI']
        totVol_pe = dict_pe_oi['totVol']
        df_pe_oi = pd.DataFrame([[stockname,'PE',totOI_pe,totVol_pe]],columns = ['SCRIP','TYPE','TOTOI','TOTVOL'])
        #print(df_pe_oi)
        df_oi = df_ce_oi.append(df_pe_oi)
        #print(df_oi)
        df_oi_data = df_oi_data.append(df_oi)
    return df_ce_data, df_pe_data,df_oi_data


# query_result = read_from_mysql('select STOCK_SYMBOL from M_OPTIONS_MASTER')


#stocklist = ['NIFTY']
