import requests
from bs4 import BeautifulSoup
import pandas as pd
from nse_tools_api import get_stock_symbol, get_top_gainers_losers
# from write_to_big_query_file import call_bigquery
# from write_to_big_query import call_bigquery_old
from output_module import write_to_csv
# import pprint
# import lxml.html as lh


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def finology_html(stock_name):
    url = f'https://ticker.finology.in/company/{stock_name}'
    print(url)
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    return url, soup


def finology_overall_rating(soup):
    html_string = soup.select_one(
        "#mainContent_ltrlOverAllRating.ratingstars.overallstars")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        rating = html_string.get("style", "000")
        # print(rating)
        rating_final = rating[9:-1]
        # print('Rating_final ' + rating_final)
        if rating_final != '':
            rating_final = float(rating_final)
    else:
        rating_final = 0
    # print(rating_final)

    # html_string = soup.select_one(
    #    "#mainContent_divValuation")
    html_string = soup.select_one(
        "#mainContent_ValuationRating")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        valuation_rating = html_string.get("style", "000")
        # print(valuation_rating)
        valuation_rating = valuation_rating[9:-1]
        # print('valuation_rating ' + valuation_rating)
        if valuation_rating != '':
            valuation_rating = float(valuation_rating)
    else:
        valuation_rating = 0
    # print(valuation_rating)

    html_string = soup.select_one(
        "#mainContent_ManagementRating")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        ownership_rating = html_string.get("style", "000")
        # print(valuation_rating)
        ownership_rating = ownership_rating[9:-1]
        if ownership_rating != '':
            ownership_rating = float(ownership_rating)
        # print('valuation_rating ' + valuation_rating)
    else:
        ownership_rating = 0
    # print(valuation_rating)

    html_string = soup.select_one(
        "#mainContent_EfficiencyRating")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        efficiency_rating = html_string.get("style", "000")
        # print(valuation_rating)
        efficiency_rating = efficiency_rating[9:-1]
        if efficiency_rating != '':
            efficiency_rating = float(efficiency_rating)
        # print('valuation_rating ' + valuation_rating)
    else:
        efficiency_rating = 0
    # print(valuation_rating)

    html_string = soup.select_one(
        "#mainContent_FinancialsRating")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        financial_rating = html_string.get("style", "000")
        # print(valuation_rating)
        financial_rating = financial_rating[9:-1]
        if financial_rating != '':
            financial_rating = float(financial_rating)
        # print('valuation_rating ' + valuation_rating)
    else:
        financial_rating = 0
    # print(valuation_rating)

    return rating_final, valuation_rating, ownership_rating, efficiency_rating, financial_rating
## 52 week high ##


def finology_52_week_high_low(soup):
    html_string = soup.select_one(
        "#mainContent_ltrl52WH")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        _52_high = html_string.getText()
    #    print(_52_high)
    else:
        _52_high = 0
    # print(_52_high)
## 52 week Low ##
    html_string = soup.select_one(
        "#mainContent_ltrl52WL")
    # print(html_string)
    # print(type(html_string))
    if html_string is not None:
        _52_low = html_string.getText()
        # print(_52_low)
    else:
        _52_low = 0
    # print(_52_low)
    return _52_high, _52_low

## Last Traded Price ##


def finology_ltp(soup):
    html_string = soup.select_one(
        "#compheader")
#    print(html_string)
   # print(type(html_string))
    if html_string is not None:
        html_string2 = soup.select_one(".col-6")
        # print(html_string2)
        html_string3 = soup.select_one(".Number")
        # print(html_string3)
        ltp = html_string3.getText()
        # print(ltp)
        # _52_low = html_string.getText()
        # print(_52_low)
    else:
        ltp = 0
        # print(ltp)
    return ltp


def finology_ratios(soup, htmlelement):
    html_string = soup.select_one(
        #    "#mainContent_divSales")
        htmlelement)

#    html_string = soup.select_one(
#        ".cardsmall")
   # print(html_string)
   # print(type(html_string))
   # print(html_string.attrs)
    if html_string is not None:
        html_string2 = html_string.select(".ratiosingle")
        # print(type(html_string2))
        duration = []
        durationvalue = []
        for html_string3 in html_string2:
            html_string4 = html_string3.select(".duration")
            # print(html_string3)
            for html_string5 in html_string4:
                duration.append(html_string5.getText())
        for html_string3 in html_string2:
            html_string4 = html_string3.select(".durationvalue")
            # print(html_string3)
            for html_string5 in html_string4:
                # print(html_string5)
                durationvalue.append(html_string5.getText()[:-1])
    else:
        durationvalue = [0, 0, 0]

    durationvalue.append(0)
    durationvalue.append(0)
    durationvalue.append(0)

    for i in range(3):
        # print(durationvalue[i])
        if is_number(durationvalue[i]):
            if durationvalue[i] != '':
                durationvalue[i] = float(durationvalue[i])
        else:
            durationvalue[i] = 0
        # print(durationvalue[i])

    # print(duration)
    # print(durationvalue)
    # ratio = dict(zip(duration, durationvalue))
    ratio = tuple(durationvalue)
    # print(ratio)
    return ratio


def finology_sector(soup):
    # print(soup)
    html_string = soup.select_one(
        "#compheader")
#    print(html_string)
#    html_string = soup.select_one(
#        ".compinfo sector mt-1")
   # print(type(html_string))
    if html_string is not None:
        html_string2 = soup.select_one("#mainContent_compinfoId")
        # print(html_string2)
        sector = html_string2.getText().replace(
            " ", "").replace('\n', "").replace('\r', "")
        # print(sector)
        sector = sector[sector.find('SECTOR:')+7:len(sector)]
        # print(sector)
    else:
        sector = 'na'
        # print(sector)
    return sector


def finology_promoter_holding(soup):
    # print(soup)
    html_string = soup.select_one(
        "#companyessentials")
    # print(html_string)
#    html_string = soup.select_one(
#        ".compinfo sector mt-1")
   # print(type(html_string))
    temp_list = []
    if html_string is not None:
        html_string2 = html_string.select(".col-6")
        for html in html_string2:
            # print('start')
            # print(html)
            temp_list.append(html.getText().replace(
                " ", "").replace('\n', "").replace('\r', ""))

        # print(temp_list)
        market_cap = temp_list[0]
        # print(market_cap)
        market_cap = market_cap[market_cap.find('Cap')+4:len(market_cap)-3]
        # print(market_cap)

        div_yield = temp_list[6]
        div_yield = div_yield[div_yield.find('Yield')+5:len(div_yield)-1]

        debt = temp_list[9]
        if debt.find('DEBT') != -1:
            debt = debt[debt.find('DEBT')+5:len(debt)-3]
        else:
            debt = 0

        promoter_holding = temp_list[10]
        promoter_holding = promoter_holding[15:-1]
        print(promoter_holding)
        if isinstance(promoter_holding,str):
            promoter_holding = 0

        promoter_holding = float(promoter_holding)

        # print(promoter_holding)
        
    else:
        promoter_holding = 0
        market_cap = 0
        div_yield = 0
        debt = 0
        # print(promoter_holding)
    return market_cap, div_yield, debt, promoter_holding


def finology_icr(soup):
    # print(soup)
    icr = 0
    html_string = soup.select_one(
        "#mainContent_divICR")
    if html_string is not None:
        html_string2 = html_string.select_one(".Number")
        if html_string2 is not None:
            icr = float(html_string2.getText())
    return icr


def finology_pledged_holing(soup):
    cols = [0, 0, 0]
    cols_first = [0, 0, 0]
    pledged_holding = 0
    html_string = soup.select_one(
        "#mainContent_DivShp")
    if html_string is not None:
        table = html_string.select_one("table")
        table_body = table.find('tbody')

        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            print(cols)
            print(cols_first)
            if cols_first[0] == 0:
                cols_first = cols
                print(cols_first)
    # print(type(cols))
    # print(cols[0])
    # print(cols[2])
    # pledged_holding_as_of = cols[0]
    #pledged_holding = float(cols[2])
        pledged_holding = float(cols_first[2])

    return pledged_holding


def finology_stock_name(soup):
    stock_name = 'na'
    html_string = soup.select_one(
        "#mainContent_ltrlCompName")
    if html_string is not None:
        stock_name = html_string.getText()
    # print(stock_full_name)
    return stock_name


def finology_peg_ratio(soup):
    peg_ratio = 0
    html_string = soup.find("div", {"id": "ratios"})
    # html_string = soup.select(
    #    "#mainContent_lblDebtEquity")
    if html_string is not None:
        html_string2 = html_string.select_one(
            ".h2"
        )
        # print(html_string)
        peg_ratio = html_string2.getText().replace(
            " ", "").replace('\n', "").replace('\r', "")
        # print(peg_ratio)
        if peg_ratio == "NA" or peg_ratio == "∞":
            peg_ratio = 0
    return peg_ratio


def finology_operating_cash_flow(soup):
    latest_cash_flow = 0
    cash_flow_minus_1 = 0
    cash_flow_minus_2 = 0
    cash_flow_minus_3 = 0
    cash_flow_minus_4 = 0
    cash_flow_trend = 'na'
    html_string = soup.select_one(
        "#mainContent_cashflows")
    # print(html_string)
    if html_string is not None:
        table = html_string.select_one("table")
        if table is not None:
            table_body = table.find('tbody')
           # print(table_body)
            rows = table_body.find_all('tr')
            if len(rows) >= 4:
                # for row in rows:
                cols = rows[4].find_all('td')
                cols = [ele.text.strip() for ele in cols]
            #    print(cols)
    # print(cols)
    # print(type(cols))
    # print(cols[0])
    # print(cols[2])
    # pledged_holding_as_of = cols[0]
                for col in cols:
                    if col != '':
                        col = float(col)
                if cols[0] < cols[1] and cols[1] < cols[2] and cols[2] < cols[3] and cols[3] < cols[4]:
                    cash_flow_trend = 'going_up'
                else:
                    cash_flow_trend = 'going_down'
                latest_cash_flow = cols[4]
                cash_flow_minus_1 = cols[3]
                cash_flow_minus_2 = cols[2]
                cash_flow_minus_3 = cols[1]
                cash_flow_minus_4 = cols[0]
    # print(cash_flow_minus_1, cash_flow_minus_2)
    return cash_flow_trend, latest_cash_flow, cash_flow_minus_1, cash_flow_minus_2, cash_flow_minus_3, cash_flow_minus_4


def finology_net_profit(soup):
    latest_net_profit = 0
    net_profit_minus_1 = 0
    net_profit_minus_2 = 0
    net_profit_minus_3 = 0
    net_profit_minus_4 = 0
    net_profit_trend = 'na'
    html_string = soup.select_one(
        "#profit")
    # print(html_string)
    if html_string is not None:
        table = html_string.select_one("table")
        if table is not None:
            table_body = table.find('tbody')
            # print(table_body)
            rows = table_body.find_all('tr')
            # print(rows)
            # print(len(rows))
            if len(rows) >= 10:
                # for row in rows:
                cols = rows[9].find_all('td')
                cols = [ele.text.strip() for ele in cols]
                print(cols)
                for col in cols:
                    if col != '':
                        col = float(col)
                    if cols[0] < cols[1] and cols[1] < cols[2] and cols[2] < cols[3] and cols[3] < cols[4]:
                        net_profit_trend = 'going_up'
                    else:
                        net_profit_trend = 'going_down'

                    latest_net_profit = cols[4]
                    net_profit_minus_1 = cols[3]
                    net_profit_minus_2 = cols[2]
                    net_profit_minus_3 = cols[1]
                    net_profit_minus_4 = cols[0]

            else:
                if len(rows) >= 9:
                    # for row in rows:
                    cols = rows[8].find_all('td')
                    cols = [ele.text.strip() for ele in cols]
                    for col in cols:
                        if col != '':
                            col = float(col)
                        if cols[0] < cols[1] and cols[1] < cols[2] and cols[2] < cols[3] and cols[3] < cols[4]:
                            net_profit_trend = 'going_up'
                        else:
                            net_profit_trend = 'going_down'

                        latest_net_profit = cols[4]
                        net_profit_minus_1 = cols[3]
                        net_profit_minus_2 = cols[2]
                        net_profit_minus_3 = cols[1]
                        net_profit_minus_4 = cols[0]

    # print(cash_flow_minus_1, cash_flow_minus_2)
    return net_profit_trend, latest_net_profit, net_profit_minus_1, net_profit_minus_2, net_profit_minus_3, net_profit_minus_4
