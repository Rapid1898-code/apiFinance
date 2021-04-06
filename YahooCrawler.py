# Read stock data from Yahoo Finance, Morningstar, Zacks, WSJ and other modules
# clean_value: clean value and returns cleaned data
# print_num_abr: returns abbreviaton of numeric value (input in thousands) - Million, Billion, Trillion
# isdigit: check if value is digit (with replacing several chars and abbreviation)
# read_dayprice: read price of a specific date - when date not available take nearest day in history from the date
# read_yahoo_summary: read yahoo summary data for stock
# read_yahoo_profile: read yahoo profile data for stock
# read_yahoo_statistics: read yahoo statistic data for stock
# read_yahoo_income_statement: read yahoo income statement data for stock
# read_yahoo_balance_sheet: read yahoo balance sheet data for stock
# read_yahoo_cashflow: read yahoo cashflow data for stock
# read_yahoo_analysis: read yahoo analysis data for stock

import RapidTechTools as rtt
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import calendar
import sys, os
import re
from selenium.webdriver.chrome.options import Options
from sys import platform
import urllib.request,urllib.error
import codecs
import csv
from datetime import datetime, timedelta
from datetime import date
from selenium.common.exceptions import NoSuchElementException
# import pycountry
import locale
import json
#locale.setlocale(category=locale.LC_ALL,locale="German")

USE_PYCOUNTRY = False

def clean_value(value, dp=".", tcorr=False, out="None"):
    """
    clean value to Float / Int / Char / None
    :param value: value which will be worked on
    :param dp: decimalpüint <.> or <,>
    :param tcorr: thousand corecction - if True numeric value will be multiplicated by 1000 - if False not
    :param out: output value in case of an invalid value
    :return: cleaned value (or error-value "N/A", None, "" defined in out)
    """
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    pattern1 = re.compile ("^[a-zA-Z]{3} [0-9]{2}, [0-9]{4}$")
    pattern2 = re.compile ("^[0-9]{4}-[0-9]{2}$")
    pattern3 = re.compile ("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    pattern4 = re.compile ("^[0-9]{1,2}/[0-9]{2}/[0-9]{4}$")
    value = rtt.replace_more(str(value).strip(), ["%","+-","+"])

    if pattern1.match(value) != None:
        value = datetime.strftime((datetime.strptime (value,"%b %d, %Y")),"%Y-%m-%d")
        return(value)
    elif pattern2.match(value) != None:
        dt = datetime.strptime (value, "%Y-%m")
        y = dt.year
        m = dt.month
        ultimo = calendar.monthrange (y, m)[1]
        value = datetime.strftime(date(y,m,ultimo), "%Y-%m-%d")
        return(value)
    elif pattern3.match(value) != None: return(value)
    elif pattern4.match (value) != None:
        value = datetime.strftime ((datetime.strptime (value, "%m/%d/%Y")), "%Y-%m-%d")
        return (value)
    elif value in ["N/A","None","nan","-","—","","∞","-∞","Invalid Date","�","undefined"]:
        if out == "None": return(None)
        elif out == "N/A": return("N/A")
    elif ("M" in value or "B" in value or "T" in value or "k" in value) and rtt.replace_more(value, [",",".","M","B","T","k","-"]).isdigit():
        if "M" in value: char = "M"
        elif "B" in value: char = "B"
        elif "T" in value: char = "T"
        elif "k" in value: char = "k"
        decimal_place = value.find(dp)
        b_place = value.find(char)
        if decimal_place == -1:
            b_place = 1
            decimal_place = 0
        #OLD: if char in ["M","B","T"]: value = value.replace(".","").replace(",","").replace(char,"")
        if char in ["M", "B", "T"]: value = rtt.replace_more(value, [".",",",char])
        # million
        if char == "M":
            for i in range (3 - (b_place - decimal_place - 1)): value = value + "0"
        # billion
        if char == "B":
            for i in range(6 - (b_place - decimal_place -1)): value = value + "0"
        # trillion
        if char == "T":
            for i in range(9 - (b_place - decimal_place -1)): value = value + "0"
        # thousand
        if char == "k":
            value = value.replace("k","")
        value = float(value)
        if tcorr: return (value * 1000)
        else: return (value)
    elif ":" in value: return(value)
    #OLD elif value.replace(",","").replace(".","").replace("-","").isdigit() == True:
    elif rtt.replace_more(value, [",",".","-"]).isdigit () == True:
        if dp == ",":
            if "." in value and "," in value: value = value.replace(".","")
            if "," in value: value = float(value.replace(",","."))
            else: value = int(value.replace(".",""))
            if tcorr: return(value * 1000)
            else: return (value)
        elif dp == ".":
            if "," in value and "." in value: value = value.replace(",","")
            if "." in value: value = float(value)
            else: value = int(value.replace(",",""))
            if tcorr: return(value * 1000)
            else: return (value)
        else: print(f"Wrong dp parameter vor {value}")
    else: return(value)

def isdigit(value):
    """
    clean value and check if it is digit
    :param value: value to be checked
    :return: TRUE if value is digit - FALSE if value is not fully digit
    """
    value = rtt.replace_more(str(value),["-",",",".","%","B","M","T"])
    return (value.isdigit())

def read_dayprice(prices,date,direction):
# read price of a specific date
# when date not available take nearest day in history from the date
    """
    read price for a specific date
    when date not available take nearest day in history from the date
    :param prices: list of prices
    :param date: date for what the price should be searched
    :param direction: if "+" then skip 1 day to future when no price is found
                      if "-" go one day in the past when nothing ist found
    :return: date and price as list (or default pair when nothing is found)
    """
    nr = 0
    while nr < 100:
        if date in prices: return [date, float(prices[date][3])]
        else:
            dt1 = datetime.strptime (date, "%Y-%m-%d")
            if direction == "+": newdate = dt1 + timedelta (days=1)
            elif direction == "-": newdate = dt1 - timedelta (days=1)
            date = datetime.strftime (newdate, "%Y-%m-%d")
            nr +=1
    return ["1900-01-01",999999999]

def read_yahoo_summary(stock,out=True,att=10):
    """
    Read summary stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :param att: number of attempts how often the reading should be repeated in case of problems
    :return: dictionary with line per value
    """
    erg = {}
    link = "https://finance.yahoo.com/quote/" + stock
    if out: print ("Reading summary web data for", stock, "...")
    erg["symbol"] = stock

    attempt = 1
    while attempt < att:
        try:
            page = requests.get (link)
            soup = BeautifulSoup (page.content, "html.parser")
            time.sleep(1)
            table = soup.find ('div', id="quote-header-info")
        except:
            pass
        if table != None: break
        if out: print ("Read attempt name failed... Try", attempt)
        time.sleep (.5 + attempt)
        attempt += 1

    if table == None: return ({})
    else: header = table.find ("h1").text

    erg["name"] = header.strip ()

    erg["currency"] = table.find (["span"]).text.strip()[-3:].upper()
    erg["exchange"] = table.find (["span"]).text.split("-")[0].strip()

    tmp_vol = soup.find('td', attrs={"data-test": "TD_VOLUME-value"})
    if tmp_vol != None: tmp_vol = tmp_vol.text.strip().replace(",","")
    if tmp_vol != "N/A" and tmp_vol != None: erg["vol"] = float (tmp_vol.replace (",", ""))
    else: erg["vol"] = "N/A"

    tmp_avg_vol = soup.find('td', attrs={"data-test": "AVERAGE_VOLUME_3MONTH-value"})
    if tmp_avg_vol != None: tmp_avg_vol = tmp_avg_vol.text.strip().replace(",","")
    if tmp_avg_vol != "N/A" and tmp_avg_vol != None: erg["avg_vol"] = float (tmp_avg_vol.replace (",", ""))
    else: erg["avg_vol"] = "N/A"

    # find price and change of day
    sp = table.find_all ("span")
    if sp != None:
        for i_idx, i in enumerate (sp):
            if i.text.replace (",", "").replace (".", "").strip ().isdigit ():
                erg["price"] = clean_value (sp[i_idx].text.strip ())
                change = sp[i_idx + 1].text.strip ()
                daychange_tmp = change.split ("(")
                if daychange_tmp != [""]:
                    erg["daychange_abs"] = clean_value (daychange_tmp[0].strip ())
                    erg["daychange_perc"] = clean_value (daychange_tmp[1][:-1].strip ())
                else:
                    erg["daychange_abs"] = "N/A"
                    erg["daychange_perc"] = "N/A"
                break
    else:
        erg["price"] = "N/A"
        erg["daychange_abs"] = "N/A"
        erg["daychange_perc"] = "N/A"

    d_r_tmp = soup.find ('td', attrs={"data-test": "DAYS_RANGE-value"})
    if d_r_tmp != None:
        d_r_tmp = d_r_tmp.text.strip ().split ('-')
        erg["day_range_from"] = clean_value(d_r_tmp[0].strip().replace(",",""))
        erg["day_range_to"] = clean_value(d_r_tmp[1].strip().replace(",",""))
    else:
        erg["day_range_from"] = "N/A"
        erg["day_range_to"] = "N/A"

    f_r_tmp = soup.find ('td', attrs={"data-test": "FIFTY_TWO_WK_RANGE-value"})
    if f_r_tmp != None and len(f_r_tmp.text.strip()) != 0:
        f_r_tmp = f_r_tmp.text.strip ().split ('-')
        erg["fifty_range_from"] = clean_value(f_r_tmp[0].strip().replace(",",""))
        erg["fifty_range_to"] = clean_value(f_r_tmp[1].strip().replace(",",""))
    else:
        erg["fifty_range_from"] = "N/A"
        erg["fifty_range_to"] = "N/A"

    tmp_marketcap = soup.find ('td', attrs={"data-test": "MARKET_CAP-value"})
    if tmp_marketcap != None: tmp_marketcap = tmp_marketcap.text.strip()
    if tmp_marketcap != "N/A" and tmp_marketcap != None: erg["marketcap"] = clean_value(tmp_marketcap) * 1000
    else: erg["marketcap"] = "N/A"

    tmp_beta = soup.find('td', attrs={"data-test": "BETA_5Y-value"})
    if tmp_beta != None: tmp_beta = tmp_beta.text.strip()
    if tmp_beta not in [None,"N/A"] and len(tmp_beta) < 10: erg["beta"] = clean_value(tmp_beta)
    else: erg["beta"] = None

    tmp_pe_ratio = soup.find ('td', attrs={"data-test": "PE_RATIO-value"})
    if tmp_pe_ratio != None: tmp_pe_ratio = tmp_pe_ratio.text.strip()
    if tmp_pe_ratio != "N/A" and tmp_pe_ratio != None: erg["pe_ratio"] = clean_value(tmp_pe_ratio)
    else: erg["pe_ratio"] = "N/A"

    tmp_eps = soup.find ('td', attrs={"data-test": "EPS_RATIO-value"})
    if tmp_eps != None: erg["eps_ratio"] = clean_value(tmp_eps.text.strip())
    else: erg["eps_ratio"] = "N/A"

    temp_div = soup.find ('td', attrs={"data-test": "DIVIDEND_AND_YIELD-value"})
    if temp_div != None: temp_div = temp_div.text.strip ().split ("(")
    if temp_div == None:
        erg["forw_dividend"] = "N/A"
        erg["div_yield"] = "N/A"
    elif "N/A" in temp_div[0].strip():
        erg["forw_dividend"] = "N/A"
        erg["div_yield"] = "N/A"
    else:
        erg["forw_dividend"] = clean_value(temp_div[0])
        erg["div_yield"] = clean_value(temp_div[1][:-1])

    tmp_oytp = soup.find ('td', attrs={"data-test": "ONE_YEAR_TARGET_PRICE-value"})
    if tmp_oytp != None: tmp_oytp = tmp_oytp.text.strip()
    if tmp_oytp != "N/A" and tmp_oytp != None: erg["price1Yest"] = float (tmp_oytp.replace (",", ""))
    else: erg["price1Yest"] = "N/A"

    tmp_next_ed = soup.find ('td', attrs={"data-test": "EARNINGS_DATE-value"})
    if tmp_next_ed == None: erg["next_earnings_date"] = "N/A"
    else:
        tmp_next_ed = tmp_next_ed.text.strip()
        if len(tmp_next_ed) > 15 and "-" in tmp_next_ed:
            tmp_next_ed = tmp_next_ed.split("-")[0].strip()
        if tmp_next_ed != "N/A":
            erg["next_earnings_date"] = datetime.strftime((datetime.strptime (tmp_next_ed,"%b %d, %Y")),"%Y-%m-%d")

    tmp_ex_dd = soup.find ('td', attrs={"data-test": "EX_DIVIDEND_DATE-value"})
    if tmp_ex_dd == None: erg["last_dividend_date"] = "N/A"
    else:
        tmp_ex_dd = tmp_ex_dd.text.strip()
        if len (tmp_ex_dd) > 15: tmp_ex_dd = "N/A"
        if tmp_ex_dd != "N/A":
            erg["last_dividend_date"] = datetime.strftime((datetime.strptime (tmp_ex_dd,"%b %d, %Y")),"%Y-%m-%d")

    return(erg)

def read_yahoo_profile(stock,out=True):
    """
    Read profile stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with line per value
    """
    erg = {}
    if out: print("Reading profile web data for",stock,"...")
    link = "https://finance.yahoo.com/quote/" + stock + "/profile?p=" + stock
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (0.5)
    erg["symbol"] = stock

    table = soup.find ('div', attrs={"class": "asset-profile-container"})
    if table == None:
        return ({})
    else:
        spans = table.find_all ("span")
    if len(spans[5].text.strip()) == 0:
        erg["empl"] = "N/A"
    else:
        erg["empl"] = int(spans[5].text.strip ().replace (",", ""))
    erg["sector"] = spans[1].text.strip ()
    erg["industry"] = spans[3].text.strip ()

    ps = table.find_all("a")

    erg["tel"] = ps[0].text
    erg["url"] = ps[1].text
    if "." in ps[1].text:
        if USE_PYCOUNTRY:
            land = ps[1].text.split(".")[-1].upper()
            if land == "COM":
                erg["country"] = "USA"
            else:
                country = pycountry.countries.get (alpha_2=land)
                if country != None:
                    erg["country"] = country.name
                else:
                    erg["country"] = "N/A"
        else:
            erg["country"] = "N/A"
    else: erg["country"] = "N/A"

    table = soup.find ('section', attrs={"class": "quote-sub-section Mt(30px)"})
    if table == None: erg["desc"] = "N/A"
    else: erg["desc"] = table.find ("p").text.strip ()

    return(erg)

# OLD def read_yahoo_statistics(stock,out=True,wait=2):
    # stock: ticker to work on
    # out: variable for debugging outputs
    # ts: number of seconds to wait between reading steps
    erg_stat = {}
    erg_val = {}

    if out: print("Reading statistic web data for",stock,"...approx 6sec...")
    link = "https://finance.yahoo.com/quote/" + stock + "/key-statistics?p=" + stock
    options = Options()
    options.add_argument('--headless')
    options.add_experimental_option ('excludeSwitches', ['enable-logging'])

    path = os.path.abspath (os.path.dirname (sys.argv[0]))
    attempt = 1
    while attempt <6:
        try:
            if platform == "win32": cd = '/chromedriver.exe'
            elif platform == "linux": cd = '/chromedriver_linux'
            elif platform == "darwin": cd = '/chromedriver'
            driver = webdriver.Chrome (path + cd, options=options)
            break
        except:
            attempt += 1
            time.sleep (1 + attempt)
            if out: print("Problems reading - try again attempt",attempt,"...")

    driver.get (link)
    time.sleep (wait)
    try:
        driver.find_element_by_name ("agree").click ()
        time.sleep (wait)
    except:
        pass
    soup = BeautifulSoup (driver.page_source, 'html.parser')
    time.sleep (wait)
    driver.quit ()

    erg_stat = {}
    erg_val = {}
    tmp_list = []
    table  = soup.find(id="Col1-0-KeyStatistics-Proxy")

    # OLD CODE SEP2020
    # if table == None:
    #     erg_stat["Return on Equity (ttm)"] = "N/A"
    #     erg_stat["Shares Outstanding"] = "N/A"
    #     erg_val["Market Cap (intraday)"] = ["N/A","N/A","N/A","N/A","N/A"]
    #     return (erg_stat,erg_val)
    if table == None:
        return ({},{})

    for e in table.find_all(["th","td"]): tmp_list.append(e.text.strip())
    for idx,cont in enumerate(tmp_list):
        if "Beta" in cont:
            tmp_list_stat = list(tmp_list[idx:])
            tmp_list_val =  list(tmp_list[:idx])
    for i in range(0,len(tmp_list_stat),2):
        matches = ["Shares Short","Short Ratio","Short % of Float","Short % of Shares Outstanding","Shares Short"]
        if any (x in tmp_list_stat[i] for x in matches):
            if "Shares Short (prior month" in tmp_list_stat[i]:
                tmp_list_stat[i] = tmp_list_stat[i].split("(")[0].strip() + " (prior month)"
            else:
                tmp_list_stat[i] = tmp_list_stat[i].split("(")[0].strip()
        if tmp_list_stat[i][-1] in ["1","2","3","4","5","6"]: tmp_list_stat[i] = tmp_list_stat[i][:len(tmp_list_stat[i])-2]
        erg_stat[tmp_list_stat[i]] = clean_value(tmp_list_stat[i+1])

    if all (x not in tmp_list_val for x in ["Price/Sales (ttm)"]):
        erg_val = {}
    else:
        for idx_header, cont_header in enumerate(tmp_list_val):
            if "Market Cap" in cont_header: break
        for i in range(0,len(tmp_list_val),idx_header):
            if tmp_list_val[i] != "":
                if tmp_list_val[i][-1] in ["1","2","3","4","5","6"]:
                    tmp_list_val[i] = tmp_list_val[i][:len(tmp_list_val[i])-2]
            else: tmp_list_val[i] = "Header"
            erg_val[tmp_list_val[i]] = tmp_list_val[i+1:i+idx_header]

    # Cleanup the values finally
    for key,val in erg_val.items():
        for idx,cont in enumerate(val):
            if key in ["Market Cap (intraday)","Enterprise Value"]:
                erg_val[key][idx] = clean_value (erg_val[key][idx],tcorr=True)
            else:
                erg_val[key][idx] = clean_value (erg_val[key][idx])
    for key,val in erg_stat.items():
        erg_stat[key] = clean_value(val)

    return (erg_stat,erg_val)

#OLD statistic function
# def read_yahoo_statistics(stock,out=True,wait=2):
#     erg_stat = {}
#     erg_val = {}
#
#     if out: print("Reading statistic web data for",stock,"...approx 6sec...")
#     link = "https://finance.yahoo.com/quote/" + stock + "/key-statistics?p=" + stock
#     options = Options()
#     options.add_argument('--headless')
#     options.add_experimental_option ('excludeSwitches', ['enable-logging'])
#
#     path = os.path.abspath (os.path.dirname (sys.argv[0]))
#     attempt = 1
#     while attempt <6:
#         try:
#             if platform == "win32": cd = '/chromedriver.exe'
#             elif platform == "linux": cd = '/chromedriver_linux'
#             elif platform == "darwin": cd = '/chromedriver'
#             driver = webdriver.Chrome (path + cd, options=options)
#             break
#         except:
#             attempt += 1
#             time.sleep (1 + attempt)
#             if out: print("Problems reading - try again attempt",attempt,"...")
#
#     driver.get (link)
#     time.sleep (2)
#     try:
#         driver.find_element_by_name ("agree").click ()
#         time.sleep (2)
#     except:
#         pass
#     soup = BeautifulSoup (driver.page_source, 'html.parser')
#     time.sleep (2)
#     driver.quit ()
#
#     erg_stat = {}
#     erg_val = {}
#     tmp_list = []
#     table  = soup.find(id="Col1-0-KeyStatistics-Proxy")
#
#     if table == None:
#         erg_stat["Return on Equity (ttm)"] = "N/A"
#         erg_stat["Shares Outstanding"] = "N/A"
#         erg_val["Market Cap (intraday)"] = ["N/A","N/A","N/A","N/A","N/A"]
#         return (erg_stat,erg_val)
#     for e in table.find_all(["th","td"]): tmp_list.append(e.text.strip())
#     for idx,cont in enumerate(tmp_list):
#         if "Beta" in cont:
#             tmp_list_stat = list(tmp_list[idx:])
#             tmp_list_val =  list(tmp_list[:idx])
#     for i in range(0,len(tmp_list_stat),2):
#         matches = ["Shares Short","Short Ratio","Short % of Float","Short % of Shares Outstanding","Shares Short"]
#         if any (x in tmp_list_stat[i] for x in matches):
#             if "Shares Short (prior month" in tmp_list_stat[i]:
#                 tmp_list_stat[i] = tmp_list_stat[i].split("(")[0].strip() + " (prior month)"
#             else:
#                 tmp_list_stat[i] = tmp_list_stat[i].split("(")[0].strip()
#         if tmp_list_stat[i][-1] in ["1","2","3","4","5","6"]: tmp_list_stat[i] = tmp_list_stat[i][:len(tmp_list_stat[i])-2]
#         erg_stat[tmp_list_stat[i]] = clean_value(tmp_list_stat[i+1])
#
#     if all (x not in tmp_list_val for x in ["Price/Sales (ttm)"]):
#         erg_val = {}
#     else:
#         for idx_header, cont_header in enumerate(tmp_list_val):
#             if "Market Cap" in cont_header: break
#         for i in range(0,len(tmp_list_val),idx_header):
#             if tmp_list_val[i] != "":
#                 if tmp_list_val[i][-1] in ["1","2","3","4","5","6"]:
#                     tmp_list_val[i] = tmp_list_val[i][:len(tmp_list_val[i])-2]
#             else: tmp_list_val[i] = "Header"
#             erg_val[tmp_list_val[i]] = tmp_list_val[i+1:i+idx_header]
#
#     # Cleanup the values finally
#     for key,val in erg_val.items():
#         for idx,cont in enumerate(val):
#             if key in ["Market Cap (intraday)","Enterprise Value"]:
#                 erg_val[key][idx] = clean_value (erg_val[key][idx],tcorr=True)
#             else:
#                 erg_val[key][idx] = clean_value (erg_val[key][idx])
#     for key,val in erg_stat.items():
#         erg_stat[key] = clean_value(val)
#
#     return (erg_stat,erg_val)

def read_yahoo_statistics(stock,out=True,wait=2):
    """
    Read statistics stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :param wait: how many seconds the processing should wait during pauses
    :return: 2 dictionaries - 1 with statistics main data and 1 statisics table data with timeslots
    """
    erg = {}
    link = "https://finance.yahoo.com/quote/" +stock + "/key-statistics?p=" + stock
    if out: print ("Reading statistics web data for", stock, "...")
    erg["symbol"] = stock

    page = requests.get (link)
    time.sleep (wait)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (wait)
    if soup == None:
        return ({},{})

    erg_stat = {}
    erg_val = {}
    tmp_list = []
    tmp_list_stat = []
    tmp_list_val = []

    for e in soup.find_all(["th","td"]): tmp_list.append(e.text.strip())
    #for i in tmp_list: print(i)        # DEBUG
    for idx,cont in enumerate(tmp_list):
        if "Beta" in cont:
            tmp_list_stat = list(tmp_list[idx:])
            tmp_list_val =  list(tmp_list[:idx])

    # check if everything is empty
    if tmp_list_stat == [] and tmp_list_val == []:
        return ({},{})
    # check if no results and redirected to symbol serach site (check header symbols)
    if "Symbol" in tmp_list_val and "Name" in tmp_list_val and "Industry / Category" in tmp_list_val:
        return ({},{})
    #print("DEBUG TmpListStat: ", tmp_list_stat)
    #print("DEBUG TmpListVal: ",tmp_list_val)

    for i in range(0,len(tmp_list_stat),2):
        matches = ["Shares Short","Short Ratio","Short % of Float","Short % of Shares Outstanding","Shares Short"]
        if any (x in tmp_list_stat[i] for x in matches):
            if "Shares Short (prior month" in tmp_list_stat[i]:
                tmp_list_stat[i] = tmp_list_stat[i].split("(")[0].strip() + " (prior month)"
            else:
                tmp_list_stat[i] = tmp_list_stat[i].split("(")[0].strip()
        if tmp_list_stat[i][-1] in ["1","2","3","4","5","6"]: tmp_list_stat[i] = tmp_list_stat[i][:len(tmp_list_stat[i])-2]
        erg_stat[tmp_list_stat[i]] = clean_value(tmp_list_stat[i+1])

    if all (x not in tmp_list_val for x in ["Price/Sales (ttm)"]):
        erg_val = {}
    else:
        for idx_header, cont_header in enumerate(tmp_list_val):
            if "Market Cap" in cont_header: break
        for i in range(0,len(tmp_list_val),idx_header):
            if tmp_list_val[i] != "":
                if tmp_list_val[i][-1] in ["1","2","3","4","5","6"]:
                    tmp_list_val[i] = tmp_list_val[i][:len(tmp_list_val[i])-2]
            else: tmp_list_val[i] = "Header"
            erg_val[tmp_list_val[i]] = tmp_list_val[i+1:i+idx_header]

    # Cleanup the values finally
    if "Header" in erg_val: erg_val["Header"][0] = "Current"
    else: return ({},{})
    for key,val in erg_val.items():
        for idx,cont in enumerate(val):
            if key in ["Market Cap (intraday)","Enterprise Value"]:
                erg_val[key][idx] = clean_value (erg_val[key][idx],tcorr=True)
            else:
                erg_val[key][idx] = clean_value (erg_val[key][idx])
    for key,val in erg_stat.items():
        if key in ["Shares Outstanding", "Float", "Shares Short","Shares Short (prior month)","Revenue (ttm)",
                   "Gross Profit (ttm)","EBITDA","Net Income Avi to Common (ttm)","Total Cash (mrq)","Total Debt (mrq)",
                   "Operating Cash Flow (ttm)","Levered Free Cash Flow (ttm)","Avg Vol (10 day)","Avg Vol (3 month)"]:
            erg_stat[key] = clean_value(val,tcorr=True)
        else:
            erg_stat[key] = clean_value(val)

    return (erg_stat,erg_val)

def read_yahoo_income_statement(stock, out=True):
    """
    Read income statement stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """
    erg = {}
    link = "https://finance.yahoo.com/quote/" + stock + "/financials?p=" + stock
    if out: print("Reading income statement web data for", stock, "...approx 6sec...")
    options = Options()
    options.add_argument('--headless')
    options.add_experimental_option ('excludeSwitches', ['enable-logging'])

    path = os.path.abspath (os.path.dirname (sys.argv[0]))
    attempt = 1
    while attempt <6:
        try:
            if platform == "win32": cd = '/chromedriver.exe'
            elif platform == "linux": cd = '/chromedriver_linux'
            elif platform == "darwin": cd = '/chromedriver'
            driver = webdriver.Chrome (path + cd, options=options)
            driver.get (link)  # Read link
            time.sleep (2)  # Wait till the full site is loaded
            try:
                driver.find_element_by_name ("agree").click ()
                time.sleep (2)
            except:
                pass
            driver.find_element_by_xpath ('//*[@id="Col1-1-Financials-Proxy"]/section/div[2]/button/div/span').click ()
            time.sleep (2)
            soup = BeautifulSoup (driver.page_source, 'html.parser')  # Read page with html.parser
            time.sleep (2)
            driver.quit ()
            break
        except NoSuchElementException:
            erg["Basic Average Shares"] = ["N/A","N/A","N/A","N/A","N/A"]
            erg["Net Income"] = ["N/A","N/A","N/A","N/A","N/A"]
            erg["Breakdown"] = ["N/A","N/A","N/A","N/A","N/A"]
            erg["Total Revenue"] = ["N/A","N/A","N/A","N/A","N/A"]
            return (erg)
        except:
            attempt += 1
            time.sleep (1 + attempt)
            if out: print("Problems reading - try again attempt",attempt,"...")

    div_id = soup.find(id="Col1-1-Financials-Proxy")
    table = soup.find (id="quote-header-info")
    erg["Header"] = [stock, "in thousands", table.find (["span"]).text.strip ()]

    list_div = []
    for e in div_id.find_all (["div"]): list_div.append (e.text.strip ())

    if all (x not in list_div for x in ["Total Revenue", "Net Income"]): return({})

    while list_div[0] != "Breakdown": list_div.pop (0)
    for i in range (len (list_div) - 1, 0, -1):
        if list_div[i].replace (".", "").replace (",", "").replace ("-", "").isdigit () or list_div[i] == "-": continue
        elif i == len (list_div) - 1: del list_div[i]
        elif len (list_div[i]) == 0: del list_div[i]
        elif len (list_div[i]) > 50: del list_div[i]
        # elif i == 0: break
        elif list_div[i] == list_div[i - 1]: del list_div[i]
        elif list_div[i + 1] in list_div[i]: del list_div[i]

    if "Total Revenue" not in list_div: return {}
    else: pos = list_div.index("Total Revenue")

    idx = 0
    while idx < len (list_div):
        if list_div[idx].replace (",", "").replace ("-", "").isdigit () == False and list_div[idx] != "-":
            idx += pos
        else:
            while list_div[idx].replace (",", "").replace ("-", "").isdigit () == True or list_div[idx] == "-":
                del list_div[idx]

    for i in range(len(list_div)-1):
        if list_div[i].replace(".", "").replace(",", "").replace("-", "").isdigit():
            list_div[i] = float(list_div[i].replace(",",""))

    idx = 0
    while idx < len (list_div):
        erg[list_div[idx]] = list_div[idx + 1:idx + pos]
        idx += pos

    for key,val in erg.items():
        for idx,cont in enumerate(val):
            erg[key][idx] = clean_value(erg[key][idx],tcorr=True)

    # skip one day future
    # when reading online the ultimo is 1 day minus in contrast to the csv-reading
    for idx,cont in enumerate(erg["Breakdown"]):
        if cont == "ttm": continue
        tmp = datetime.strptime(cont, "%Y-%m-%d") + timedelta(days=1)
        erg["Breakdown"][idx] = datetime.strftime(tmp, "%Y-%m-%d")

    return (erg)

def read_yahoo_balance_sheet(stock, out=True):
    """
    Read balance sheet stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """
    erg = {}
    link = "https://finance.yahoo.com/quote/" + stock + "/balance-sheet?p=" + stock

    if out: print("Reading balance sheet web data for", stock, "...approx 6sec...")
    options = Options()
    options.add_argument('--headless')
    options.add_experimental_option ('excludeSwitches', ['enable-logging'])

    path = os.path.abspath (os.path.dirname (sys.argv[0]))
    attempt = 1
    while attempt <6:
        try:
            if platform == "win32": cd = '/chromedriver.exe'
            elif platform == "linux": cd = '/chromedriver_linux'
            elif platform == "darwin": cd = '/chromedriver'
            driver = webdriver.Chrome (path + cd, options=options)
            driver.get (link)
            time.sleep (2)
            try:
                driver.find_element_by_name ("agree").click ()
                time.sleep (2)
            except:
                pass
            driver.find_element_by_xpath ('//*[@id="Col1-1-Financials-Proxy"]/section/div[2]/button/div/span').click ()
            time.sleep (2)
            soup = BeautifulSoup (driver.page_source, 'html.parser')
            time.sleep (2)
            driver.quit ()
            break
        except NoSuchElementException:
            erg["Stockholders' Equity"] = ["N/A","N/A","N/A","N/A","N/A"]
            erg["Total Assets"] = ["N/A","N/A","N/A","N/A","N/A"]
            erg["Breakdown"] = ["N/A", "N/A", "N/A", "N/A", "N/A"]
            return (erg)
        except:
            attempt += 1
            time.sleep (1 + attempt)
            if out: print("Problems reading - try again attempt",attempt,"...")

    table = soup.find (id="quote-header-info")
    erg["Header"] = [stock, "in thousands", table.find (["span"]).text.strip ()]
    table = soup.find (id="Col1-1-Financials-Proxy")

    list_div = []
    for e in table.find_all (["div"]): list_div.append (e.text.strip ())

    if "Breakdown" not in list_div: return({})

    while list_div[0] != "Breakdown": list_div.pop(0)
    for i in range (len (list_div) - 1, 0, -1):
        if list_div[i].replace (".", "").replace (",", "").replace ("-", "").isdigit () or list_div[i] == "-": continue
        elif i == len (list_div) - 1: del list_div[i]
        elif len (list_div[i]) == 0: del list_div[i]
        elif len (list_div[i]) > 50: del list_div[i]
        elif i == 0: break
        elif list_div[i] == list_div[i - 1]: del list_div[i]
        elif list_div[i + 1] in list_div[i]: del list_div[i]

    # Eliminate numeric entries on the false position
    if "Total Assets" in list_div: pos = list_div.index ("Total Assets")
    else: return({})
    idx = 0
    # If the element is a Digit - this is wrong and the elements got deleted as long they are an digit
    while idx < len (list_div):
        # When Non-Digit - jump POS forward
        if list_div[idx].replace (",", "").replace ("-", "").replace (".", "").isdigit () == False and list_div[idx] != "-":
            idx += pos
        else:
            while list_div[idx].replace (",", "").replace ("-", "").replace (".", "").isdigit () == True or list_div[idx] == "-":
                del list_div[idx]
                # if the wrong digit values are at the very end - check if end of list is reached
                if idx == len(list_div):
                    break

    for i in range(len(list_div)-1):
        if list_div[i].replace(".", "").replace(",", "").replace("-", "").isdigit():
            list_div[i] = float(list_div[i].replace(",",""))

    idx = 0
    while idx < len (list_div):
        erg[list_div[idx]] = list_div[idx + 1:idx + pos]
        idx += pos

    for key,val in erg.items():
        for idx,cont in enumerate(val):
            erg[key][idx] = clean_value(erg[key][idx],tcorr=True)

    # skip one day future
    # when reading online the ultimo is 1 day minus in contrast to the csv-reading
    for idx,cont in enumerate(erg["Breakdown"]):
        if cont == "ttm": continue
        tmp = datetime.strptime(cont, "%Y-%m-%d") + timedelta(days=1)
        erg["Breakdown"][idx] = datetime.strftime(tmp, "%Y-%m-%d")

    return (erg)

def read_yahoo_cashflow(stock, out=True):
    """
    Read cashflow stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """
    erg = {}
    if out: print("Reading cashflow web data for", stock, "...approx 6sec...")
    link = "https://finance.yahoo.com/quote/" + stock + "/cash-flow?p=" + stock
    options = Options()
    options.add_argument('--headless')
    options.add_experimental_option ('excludeSwitches', ['enable-logging'])

    path = os.path.abspath (os.path.dirname (sys.argv[0]))
    attempt = 1
    while attempt <6:
        try:
            if platform == "win32": cd = '/chromedriver.exe'
            elif platform == "linux": cd = '/chromedriver_linux'
            elif platform == "darwin": cd = '/chromedriver'
            driver = webdriver.Chrome (path + cd, options=options)
            driver.get (link)  # Read link
            time.sleep (2)  # Wait till the full site is loaded
            try:
                driver.find_element_by_name ("agree").click ()
                time.sleep (2)
            except:
                pass
            driver.find_element_by_xpath ('//*[@id="Col1-1-Financials-Proxy"]/section/div[2]/button/div/span').click ()
            time.sleep (2)
            soup = BeautifulSoup (driver.page_source, 'html.parser')  # Read page with html.parser
            time.sleep (2)
            driver.quit ()
            break
        except NoSuchElementException:
            return (erg)
        except:
            attempt += 1
            time.sleep (1 + attempt)
            if out: print("Problems reading - try again attempt",attempt,"...")

    div_id = soup.find(id="Col1-1-Financials-Proxy")
    table  = soup.find(id="quote-header-info")
    erg["Header"] = [stock,"in thousands",table.find(["span"]).text.strip()]

    list_div = []
    for e in div_id.find_all (["div"]): list_div.append (e.text.strip ())

    if all (x not in list_div for x in ["Operating Cash Flow", "Free Cash Flow", "Cash Dividends Paid"]): return({})

    while list_div[0] != "Breakdown": list_div.pop (0)
    for i in range (len (list_div) - 1, 0, -1):
        if list_div[i].replace (".", "").replace (",", "").replace ("-", "").isdigit () or list_div[i] == "-": continue
        elif i == len (list_div) - 1: del list_div[i]
        elif len (list_div[i]) == 0: del list_div[i]
        elif len (list_div[i]) > 50: del list_div[i]
        elif i == 0: break
        elif list_div[i] == list_div[i - 1]: del list_div[i]
        elif list_div[i + 1] in list_div[i]: del list_div[i]

    # read counts of columns with values
    if "Operating Cash Flow" not in list_div:
        # if there is no "operating cash flow" entry - generate one at the right positon
        for i in range(len(list_div)):
            pattern = re.compile ("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$")  # looks like m/dd/yyyy
            if list_div[i] not in ["ttm","Breakdown"] and pattern.match(list_div[i]) == None:
                break
        list_div.insert(i,"Operating Cash Flow")
    pos = list_div.index ("Operating Cash Flow")
    # if operating cashflow on wrong position
    if pos > 7:
        for i in range(len(list_div)):
            pattern = re.compile ("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$")  # looks like m/dd/yyyy
            if list_div[i] not in ["ttm","Breakdown"] and pattern.match(list_div[i]) == None:
                break
        list_div[pos] = "Opearting Cash Flow 2"
        list_div.insert (i, "Operating Cash Flow")
        pos = i

    idx = 0
    while idx < len (list_div):
        if list_div[idx].replace (",", "").replace ("-", "").isdigit () == False and list_div[idx] != "-":
            idx += pos
        else:
            while list_div[idx].replace (",", "").replace ("-", "").isdigit () == True or list_div[idx] == "-":
                del list_div[idx]

    idx = 0
    while idx < len (list_div):
        erg[list_div[idx]] = list_div[idx + 1:idx + pos]
        idx += pos

    for key,val in erg.items():
        for idx,cont in enumerate(val):
            erg[key][idx] = clean_value(erg[key][idx],tcorr=True)

    # skip one day future
    # when reading online the ultimo is 1 day minus in contrast to the csv-reading
    for idx,cont in enumerate(erg["Breakdown"]):
        if cont == "ttm": continue
        tmp = datetime.strptime(cont, "%Y-%m-%d") + timedelta(days=1)
        erg["Breakdown"][idx] = datetime.strftime(tmp, "%Y-%m-%d")

    return (erg)

#OLD analysis function
# def read_yahoo_analysis(stock, out=True):
#     erg = {}
#     link = "https://finance.yahoo.com/quote/" + stock + "/analysis?p=" + stock
#     if out: print("Reading analysis web data for", stock, "...approx 6sec...")
#     options = Options()
#     options.add_argument('--headless')
#     options.add_experimental_option ('excludeSwitches', ['enable-logging'])
#
#     path = os.path.abspath (os.path.dirname (sys.argv[0]))
#     attempt = 1
#     while attempt <6:
#         try:
#             if platform == "win32": cd = '/chromedriver.exe'
#             elif platform == "linux": cd = '/chromedriver_linux'
#             elif platform == "darwin": cd = '/chromedriver'
#             driver = webdriver.Chrome (path + cd, options=options)
#             break
#         except:
#             attempt += 1
#             time.sleep (1 + attempt)
#             if out: print("Problems reading Analysis - try again attempt",attempt,"...")
#
#     attempt = 1
#     table = None
#     while attempt < 5 and table == None:
#         try:
#             driver.get (link)
#             time.sleep (2)
#             try:
#                 driver.find_element_by_name ("agree").click ()
#                 time.sleep (2)
#             except:
#                 pass
#             soup = BeautifulSoup (driver.page_source, 'html.parser')
#             time.sleep (3)
#             driver.quit ()
#             table = soup.find(id="YDC-Col1")
#             break
#         except:
#             attempt += 1
#             time.sleep (1 + attempt)
#             print("Problems reading Analysis - try again attempt",attempt,"...")
#
#     if table == None: return ({})
#
#     erg = {}
#     list_table = []
#     for e in table.find_all (["th", "td"]):
#         if e.text.strip () == "0": list_table.append("N/A")
#         else: list_table.append(clean_value(e.text.strip ()))
#     for i in range (0, len (list_table), 5): erg[list_table[i]] = list_table[i + 1:i + 5]
#
#     if "Earnings Estimate" in erg: return (erg)
#     else: return ({})
#
#     for key,val in erg.items():
#         for idx,cont in enumerate(val):
#             erg[key][idx] = clean_value(erg[key][idx])
#
#     for key in ["Earnings History", "EPS Est.", "EPS Actual", "Difference", "Surprise %"]:
#         if key in erg: erg[key].reverse()

def read_yahoo_analysis(stock, out=True):
    """
    Read analysis stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """
    erg = {}
    link = "https://finance.yahoo.com/quote/" + stock + "/analysis?p=" + stock
    if out: print("Reading analysis web data for", stock, "...approx 6sec...")

    attempt = 1
    table = None
    while attempt < 5 and table == None:
        try:
            page = requests.get (link)
            time.sleep (1)
            soup = BeautifulSoup (page.content, "html.parser")
            time.sleep (1)
            table = soup.find(id="YDC-Col1")
            break
        except:
            attempt += 1
            time.sleep (1 + attempt)
            print("Problems reading Analysis - try again attempt",attempt,"...")

    if table == None: return ({})

    erg = {}
    list_table = []
    for e in table.find_all (["th", "td"]):
        if e.text.strip () == "0": list_table.append("N/A")
        else: list_table.append(clean_value(e.text.strip ()))
    for i in range (0, len (list_table), 5): erg[list_table[i]] = list_table[i + 1:i + 5]

    for key,val in erg.items():
        for idx,cont in enumerate(val):
            if key in ["Avg. Estimate","Low Estimate","High Estimate","Year Ago Sales"]:
                erg[key][idx] = clean_value (erg[key][idx],tcorr=True)
            else:
                erg[key][idx] = clean_value(erg[key][idx])

    #for key in ["Earnings History", "EPS Est.", "EPS Actual", "Difference", "Surprise %"]:
    #    if key in erg: erg[key].reverse()

    if "Earnings Estimate" in erg: return (erg)
    else: return ({})

def read_yahoo_analysis_rating(stock, out=True):
    """
    Read analysis rating data from yahoo
    (currently not in use - morely used WSJ analysis rating)
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: 1 dictionary with values
    """
    count = 0
    while count < 5:
        erg = {}
        link = "https://finance.yahoo.com/quote/" + stock + "/analysis?p=" + stock
        if out: print("Reading analysis rating web data for", stock, "...approx 10sec...")
        options = Options()
        #options.add_argument('--headless')

        path = os.path.abspath (os.path.dirname (sys.argv[0]))
        attempt = 1
        while attempt < 6:
            try:
                if platform == "win32":
                    cd = '/chromedriver.exe'
                elif platform == "linux":
                    cd = '/chromedriver_linux'
                elif platform == "darwin":
                    cd = '/chromedriver'
                driver = webdriver.Chrome (path + cd, options=options)
                break
            except:
                attempt += 1
                time.sleep (1 + attempt)
                if out: print ("Problems reading - try again attempt", attempt, "...")

        #driver.minimize_window()

        driver.get (link)
        time.sleep (2)
        try:
            driver.find_element_by_name ("agree").click ()
            time.sleep (2)
        except:
            pass
        soup = BeautifulSoup (driver.page_source, 'html.parser')
        time.sleep (6)
        driver.quit ()

        tmp_list = []

        count += 1
        table = soup.find (id="mrt-node-Col2-4-QuoteModule")
        for i in table.find_all (["div"]):
            if len(i.text.strip()) <= 3 and len(i.text.strip()) >= 1:
                tmp_list.append(i.text.strip())
        if tmp_list != []:
            erg["Recommendation Rating"] = [tmp_list[0],"1 Strong Buy to 5 Sell"]
            break
        else:
            if out: print("Try to read again...")

    return (erg)

def read_tasi_index(read_to=datetime(1950,1,1), out=True):
#Parameter read_to: date in datetime-format (year,month,day)
    """
    read data from tasi index
    :param read_to: date in datetime-format (year,month,day)
    :param out: when True then output some status informations during program running
    :return: return tasi index information
    """
    erg = {}
    list_erg = []
    link = "https://www.tadawul.com.sa/wps/portal/tadawul/markets/equities/indices/today/!ut/p/z1/pZG7DoJAEEW_xYKWGQTx0W1MRAyakAjiNgbMihjYVR7i54tYaXTVON1MzinuHaAQAOXhOYnDMhE8TJt9Tc1NtzseaEMDHXT6GhLTQtudG7o1QVjJALQ0oD_5lr3oI3HJ1J_4y8bX__PR-M7HN0Pws0-lyEKTA21Fj8CLDqTALWQLSFLMgMapiO4fJTzSBzHQnO1YznK1ypvzviyPxUhBBeu6VlkqThXjhboVmYIJj8TllbkXRQnBswDHzPO8ABP70EvPDulcAQ1PZW4!/p0/IZ7_NHLCH082KGN530A68FC4AN2O63=CZ6_22C81940L0L710A6G0IQM43GF0=MEtabIndex!Performance=chart_tasi_current_sector!TASI==/?"
    if out: print("Reading historical index price web data for Index TASI...")
    options = Options()
    options.add_argument('--headless')
    options.add_experimental_option ('excludeSwitches', ['enable-logging'])

    path = os.path.abspath (os.path.dirname (sys.argv[0]))

    attempt = 1
    while attempt < 6:
        try:
            if platform == "win32": cd = '/chromedriver.exe'
            elif platform == "linux": cd = '/chromedriver_linux'
            elif platform == "darwin": cd = '/chromedriver'
            driver = webdriver.Chrome (path + cd, options=options)
            driver.get (link)  # Read link
            time.sleep (1)
            driver.find_element_by_xpath ('//*[@id="performance_wrapper"]/div[1]/div/ul/li[6]').click ()
            time.sleep (1)
            soup = BeautifulSoup (driver.page_source, 'html.parser')  # Read page with html.parser
            break
        except NoSuchElementException:
            if out: print("Error - No Such Element")
            return (erg)
        except:
            attempt += 1
            time.sleep (1 + attempt)
            if out: print ("Problems reading - try again attempt", attempt, "...")

    initial = True
    tmp_dt = 0

    while True:
        if initial == False:
            driver.find_element_by_xpath ('//*[@id="pageing_next"]').click ()
            time.sleep (.5)
            soup = BeautifulSoup (driver.page_source, 'html.parser')  # Read page with html.parser
        else: initial = False

        table = soup.find (id="performance")
        for e in table.find_all (["tr","td"]):
            if len(e.text.strip()) < 20: list_erg.append (e.text.strip ())

        #print("Working on",list_erg[0])
        for i in range(0,len(list_erg)-1,6):
            dt = datetime.strftime ((datetime.strptime (list_erg[i], "%Y/%m/%d")), "%Y-%m-%d")
            erg[dt] = [clean_value(list_erg[1]), clean_value(list_erg[2]), clean_value(list_erg[3]),
                       clean_value(list_erg[4]),"", clean_value(list_erg[5])]

        dt_check = datetime.strptime(list_erg[0], "%Y/%m/%d")

        if tmp_dt == list_erg[0] or dt_check < read_to: return(erg)
        tmp_dt = list_erg[0]
        list_erg = []

def read_yahoo_histprice(stock, read_to=datetime(1950,1,1), out=True):
    """
    read historic stock prices
    :param stock: ticker-symbol which should be read
    :param read_to: datetime - how long in the past the prices should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with daily stock prices for the ticker
    """
    if stock.upper () == "AEX25": stock = "%5Eaex"
    if stock.upper () == "ASX200": stock = "%5EAXJO"
    if stock.upper () == "ATX": stock = "%5EATX"
    if stock.upper () == "BEL20": stock = "%5EBFX"
    if stock.upper () == "CAC40": stock = "%5EFCHI"
    if stock.upper () == "DAX": stock = "%5EGDAXI"
    if stock.upper () == "DOWJONES": stock = "%5EDJI"
    if stock.upper () == "EUROSTOXX50": stock = "%5ESTOXX50E"
    if stock.upper () == "EUROSTOXX600": stock = "%5Estoxx"
    if stock.upper () == "FTSE100": stock = "%5EFTSE"
    if stock.upper () == "HANGSENG": stock = "%5EHSI"
    if stock.upper () == "IBEX35": stock = "%5EIBEX"
    if stock.upper () == "MDAX": stock = "%5EMDAXI"
    # if stock.upper () == "MIB": stock = "%FTSEMIB.MI"     # currently no prices available on yahoo finance
    if stock.upper () == "NASDAQ": stock = "%5EIXIC"
    if stock.upper () == "NIKKEI225": stock = "%5EN225"
    if stock.upper () == "SDAX": stock = "%5ESDAXI"
    if stock.upper () == "SMI": stock = "%5ESSMI"
    if stock.upper () == "SP500": stock = "%5EGSPC"
    if stock.upper () == "TSX": stock = "%5EGSPTSE"

    if stock.upper() == "TASI": return(read_tasi_index(read_to))

    erg = {}
    tmp_list = []
    dt_readto = datetime.strftime (read_to, "%Y-%m-%d")

    #generate iso-format for actual date
    #OLD iso_dt = datetime.fromisoformat (str (datetime.now()- timedelta(days=1))).timestamp ()
    iso_dt = datetime.fromisoformat (str (datetime.now ())).timestamp ()
    iso_dt = str(int(round (iso_dt, 0)))
    #print("DEBUG-ISODATE:",iso_dt)

    link = "https://query1.finance.yahoo.com/v7/finance/download/" + stock + "?period1=345427200&period2=" + iso_dt + "&interval=1d&events=history"
    if out: print("Reading historical share price data for", stock, "...")
    try:
        ftpstream = urllib.request.urlopen(link)
    except urllib.error.URLError:
        print("CSV-Link can not be opened...")
        return erg

    csvfile = csv.reader(codecs.iterdecode(ftpstream, 'utf-8'))
    for row in csvfile:
        #print("DEBUG-ROW:",row)
        if row[1] != "null": tmp_list.append(row)
    tmp_list.reverse()
    #print("DEBUG-TMP_LIST:",tmp_list)

    erg[tmp_list[-1][0]] = tmp_list[-1][1:]
    for i in range(len(tmp_list)):
        if dt_readto > tmp_list[i][0]: break
        erg[tmp_list[i][0]] = tmp_list[i][1:]

    for key, val in erg.items ():
        for i_idx,i_cont in enumerate(val):
            erg[key][i_idx] = clean_value (i_cont,dp=".")

    return erg

def read_yahoo_histdividends(stock, read_to=datetime(1950,1,1), out=True):
    """
    read historic dividends payments from the stock
    # from eg. here: https://finance.yahoo.com/quote/AAPL/history?period1=1568445190&period2=1600067590&interval=div%7Csplit&filter=div&frequency=1d
    :param stock: ticker-symbol which should be read
    :param read_to: datetime - how long in the past the dividends should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with historic dividend payouts
    """
    erg = {}
    tmp_list = []

    #generate iso-format for actual date
    iso_dt = datetime.fromisoformat (str (datetime.now()- timedelta(days=1))).timestamp ()
    iso_dt = str(int(round (iso_dt, 0)))

    link = "https://query1.finance.yahoo.com/v7/finance/download/" + stock + "?period1=345427200&period2=" + iso_dt + "&interval=1d&events=div"
    #print("DEBUG Link:", link)

    if out: print("Reading historical dividends data for", stock, "...")
    try:
        ftpstream = urllib.request.urlopen(link)
    except urllib.error.URLError:
        return erg

    csvfile = csv.reader(codecs.iterdecode(ftpstream, 'utf-8'))
    for row in csvfile:
        if row[1] != "null": tmp_list.append(row)
    tmp_list.reverse()
    erg[tmp_list[-1][0]] = tmp_list[-1][1:]
    for i in range(len(tmp_list)):
        erg[tmp_list[i][0]] = clean_value(tmp_list[i][1])

    # sort dict
    erg = {k: v for k, v in sorted (erg.items (), key=lambda item: item[0], reverse=True)}

    return erg

def read_yahoo_histsplits (stock, read_to=datetime(1950,1,1), out=True):
    """
    read historic stock splits
    # from eg. here: https://finance.yahoo.com/quote/AAPL/history?period1=1568445190&period2=1600067590&interval=div%7Csplit&filter=split&frequency=1d
    :param stock: ticker-symbol which should be read
    :param read_to: datetime - how long in the past the splits should be searched
    :param out: when True then output some status informat
    :return: dictionary with historic stock splits
    """
    erg = {}
    tmp_list = []

    #generate iso-format for actual date
    iso_dt = datetime.fromisoformat (str (datetime.now()- timedelta(days=1))).timestamp ()
    iso_dt = str(int(round (iso_dt, 0)))

    link = "https://query1.finance.yahoo.com/v7/finance/download/" + stock + "?period1=345427200&period2=" + iso_dt + "&interval=1d&events=split"
    if out: print("Reading historical split data for", stock, "...")
    try:
        ftpstream = urllib.request.urlopen(link)
    except urllib.error.URLError:
        return erg

    csvfile = csv.reader(codecs.iterdecode(ftpstream, 'utf-8'))
    for row in csvfile:
        if row[1] != "null": tmp_list.append(row)
    tmp_list.reverse()
    erg[tmp_list[-1][0]] = tmp_list[-1][1:]
    for i in range(len(tmp_list)):
        erg[tmp_list[i][0]] = tmp_list[i][1:]

    # sort dict
    erg = {k: v for k, v in sorted (erg.items (), key=lambda item: item[0], reverse=True)}

    return erg

def read_ipos(read_from=datetime.today(), read_to=datetime(1950,1,1), usdOnly = False):
    """
    read ipo ticker symbols and store in list
    :param read_from: starting date from which the ipos should be read
    :param read_to: end date to which the ipos should be read
    :param usdOnly: read only tickers without "." in ticker-name
    :return: list with ipos ticker symbols as list
    """
    list_erg = []
    # find last sunday for datetime.today or datetime-parameter from function
    sunday = read_from - timedelta(days=read_from.isoweekday())

    while sunday > read_to:
        dt1 = datetime.strftime(sunday, "%Y-%m-%d")
        dt2 = datetime.strftime(sunday+timedelta(days=6), "%Y-%m-%d")
        for i in range(7):
            dt3 = datetime.strftime(sunday+timedelta(days=i), "%Y-%m-%d")
            print("Working on",dt3)
            link = "https://finance.yahoo.com/calendar/ipo?from=" + dt1 + "&to=" + dt2 + "&day=" + dt3
            #print("DEBUG LINK:",link)
            #print("DEBUG DT3:",dt3)

            page = requests.get (link)
            soup = BeautifulSoup (page.content, "html.parser")
            time.sleep (1)

            check = soup.find_all ('span', attrs={"data-reactid": "7"})
            if "We couldn't find" in check[1].text: continue

            table = soup.find (id="fin-cal-table")
            for e in table.find_all (["td"]):
                f = e.find("a")
                if f != None and len(f) > 0 and f.text not in list_erg:
                    if usdOnly:
                        if "." not in f.text:
                            list_erg.append((f.text,dt3))
                    else:
                        list_erg.append((f.text,dt3))
            print("DEBUG list_erg: ",list_erg)
        sunday -= timedelta (days=7)
    return(list_erg)

def read_yahoo_earnings_cal(stock, out=True):
    """
    read earnings calender for stock
    future earnings calls and past earnings calls with eps results
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with line per date and different values in columns
    """
    erg = {}
    erg["Header"] = ["Symbol", "Company", "EPS_Estimate", "Reported_EPS", "Surprise"]

    link = "https://finance.yahoo.com/calendar/earnings/?symbol=" + stock
    if out: print("Reading earnings calender web data for",stock,"...")
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (4)

    tmp_list = []
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    table = soup.find (id="fin-cal-table")
    for row in soup.find_all ("td"): tmp_list.append (row.text.strip ())
    idx = 0

    #for i in tmp_list: print(i)        # DEBUG

    while idx < len (tmp_list):
        # cut timezone at the end of the string - sometimes 3chars - sometimes 4chars
        tmp = tmp_list[idx + 2]
        ampm_idx = tmp.find ("AM")
        if ampm_idx == -1: ampm_idx = tmp.find ("PM")
        tz_cut = ampm_idx + 2
        tmp = tmp[:tz_cut]
        #print("DEBUG TMP2: ",tmp)

        dt1 = datetime.strptime (tmp, "%b %d, %Y, %I %p")
        dt2 = datetime.strftime (dt1, "%Y-%m-%d")
        erg[dt2] = [clean_value(tmp_list[idx + 0]),
                    clean_value(tmp_list[idx + 1]),
                    clean_value(tmp_list[idx + 3]),
                    clean_value(tmp_list[idx + 4]),
                    clean_value(tmp_list[idx + 5])]
        idx += 6

    # if there is only the header and no entries => set the erg-result to {}
    if len(list(erg.keys())) == 1: erg = {}

    return(erg)

def read_yahoo_options(stock,read_to=datetime(2099,1,1), what="ALL", out=True):
# Read options stock data from yahoo
    """
    read options for stock
    :param stock: ticker-symbol which should be read
    :param read_to: how long int future the options should be read
    :param what: if "ALL" read puts/calls - if "Puts" read only Puts - if "Calls" read only Calls
    :param out: when True then output some status informations during program running
    :return: dictionary with line per dates and different informations in columns
    """
    erg = {}
    if out: print("Reading options web data for",stock,"...")
    link = "https://finance.yahoo.com/quote/" + stock + "/options"
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (1)

    # read different dates from drop box
    table = soup.find_all ('option')
    dates = []
    for i in table:
        val = int(i.get("value").strip())
        dt = datetime.fromtimestamp(val * 1000 / 1e3)
        #print("DEBUG Text: ", i.text)
        #print("DEBUG Value: ",val)
        #print("DEBUG Date: ",dt)
        dates.append([dt,val])
    dates.sort()
    #print("DEBUG: ",dates)
    #for i in dates: print(i)    #DEBUG

    # read puts and calls for stock
    erg["header"] = ["call_put","date","lasttrade_date","strike","last_price","bid","ask","change","change_perc","vol","open_interest","implied_volatility"]

    # read through possible dates
    for dt in dates:
        # if actual date > date-parameter: return results
        if dt[0] > read_to + timedelta(days=1): break
        print("Working for stock",stock,"on date",dt[0],"...")

        last_strike = 0
        option = "Calls"
        link = "https://finance.yahoo.com/quote/" + stock + "/options?date=" + str(dt[1]) + "&p=" + stock
        #print("DEBUG Link: ",link)
        page = requests.get (link)
        soup = BeautifulSoup (page.content, "html.parser")
        time.sleep (1)
        table = soup.find ('div', id="Main")
        tr = table.find_all("tr")
        for i in tr:
            dt_str = datetime.strftime(dt[0], "%Y-%m-%d")
            td = i.find_all("td")
            if td != None and len(td) > 0:
                row = [option, dt_str]
                contract = td[0].text.strip()
                for j_idx, j_cont in enumerate(td[1:]):
                    if j_idx == 1:
                        if float(j_cont.text.strip().replace(".","").replace(",","")) >= last_strike:
                            last_strike = float(j_cont.text.strip().replace(".","").replace(",",""))
                        else:
                            option = "Puts"
                            row[0] = "Puts"
                    row.append(j_cont.text.strip())
                if what == "ALL" or (what == "Calls" and option == "Calls") or (what == "Puts" and option == "Puts"):
                    erg[contract] = row

        for key,val in erg.items():
            for idx,cont in enumerate(val):
                erg[key][idx] = clean_value(erg[key][idx])

    return(erg)

def read_wsj_rating(stock, out=True):
    """
    read rating for stock according to wsj wall street journal
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with informations per line and timespans in columns
    """
    erg = {}
    if out: print("Reading rating web data for", stock, "...approx 3sec...")
    if ".DE" in stock: country = "XE/XETR/"
    elif ".AS" in stock: country = "NL/XAMS/"
    elif ".AX" in stock: country = "AU/XASX/"
    elif ".BR" in stock: country = "BE/XBRU/"
    elif ".CO" in stock: country = "DK/XCSE/"
    elif ".FI" in stock: country = "FI/XHEL/"
    elif ".HE" in stock: country = "FI/XHEL/"
    elif ".HK" in stock: country = "HK/XHKG/"
    elif ".IR" in stock: country = "IE/XDUB/"
    elif ".KS" in stock: country = "KR/XKRX/"
    elif ".LS" in stock: country = "PT/XLIS/"
    elif ".L" in stock: country = "UK/XLON/"
    elif ".MC" in stock: country = "ES/MABX/"
    elif ".MI" in stock: country = "IT/XMIL/"
    elif ".OL" in stock: country = "NO/XOSL/"
    elif ".PA" in stock: country = "FR/XPAR/"
    elif ".PR" in stock: country = "CZ/XPRA/"
    elif ".ST" in stock: country = "SE/XSTO/"
    elif ".SW" in stock: country = "CH/XSWX/"
    elif ".TO" in stock: country = "CA/XTSE/"
    elif ".T" in stock: country = "JP/XTKS/"
    elif ".VI" in stock: country = "AT/XWBO/"
    elif ".VX" in stock: country = "CH/XSWX/"
    else: country = ""

    stock = stock.split(".")[0]
    link = "https://www.wsj.com/market-data/quotes/" + country + stock + "/research-ratings"

    attempt = 1
    while attempt < 10:
        try:
            options = Options ()
            options.add_argument ('--headless')
            options.add_experimental_option ('excludeSwitches', ['enable-logging'])
            path = os.path.abspath (os.path.dirname (sys.argv[0]))
            if platform == "win32":
                cd = '/chromedriver.exe'
            elif platform == "linux":
                cd = '/chromedriver_linux'
            elif platform == "darwin":
                cd = '/chromedriver'
            driver = webdriver.Chrome (path + cd, options=options)
            driver.get (link)
            break
        except:
            if out: print("TRY AGAIN... Read WJS-Data...",attempt)
            attempt += 1

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit ()
    div_id = soup.find(id="historicalCol")

    if div_id == None:
        erg["Rating"] = ["N/A","N/A","N/A","N/A"]
        for i in ["Buy","Overweight","Hold","Underweight","Sell"]: erg[i] = ["N/A","N/A","N/A","N/A"]
        return (erg)

    tmp = []
    for row in div_id.find_all("span"):
        if len(row.text.strip()) != 0: tmp.append(row.text.strip())

    if "Buy" not in tmp:
        erg["Rating"] = ["N/A","N/A","N/A","N/A"]
        for i in ["Buy","Overweight","Hold","Underweight","Sell"]: erg[i] = ["N/A","N/A","N/A","N/A"]
        return (erg)

    erg["Header"] = ["Current","1 Month Ago","3 Month Ago"]
    idx_tmp = 0
    while idx_tmp < len(tmp):
        if tmp[idx_tmp] in ["Buy","Overweight","Hold","Underweight","Sell"]:
            erg[tmp[idx_tmp]] = [int(tmp[idx_tmp+3]),int(tmp[idx_tmp+2]),int(tmp[idx_tmp+1])]
            idx_tmp += 4
        else: idx_tmp += 1

    rating_hist = []
    rating_opinions = []
    for idx_head,cont_head in enumerate(erg["Header"]):
        rat = 0
        sum_rat = 0
        count_rat = 0
        for idx,cont in enumerate(["Buy","Overweight","Hold","Underweight","Sell"]):
            sum_rat += erg[cont][idx_head] * (idx+1)
            count_rat += erg[cont][idx_head]
        if count_rat != 0: rat = round(sum_rat / count_rat,2)
        else: count_rat = 0
        if idx_head == 0: erg["Rating"] = [rat,'1Buy to 5Sell',count_rat,"Analyst Opinions"]
        rating_hist.append(rat)
        rating_opinions.append(count_rat)
    erg["RatingHist"] = rating_hist
    erg["RatingOpinions"] = rating_opinions

    for key,val in erg.items():
        for idx,cont in enumerate(val):
            erg[key][idx] = clean_value(erg[key][idx])

    return (erg)

def read_morningstars_financials(stock_ms, out=True):
    """
    Read morningstar stock data from yahoo
    :param stock: ticker-code from morningstar which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """
    erg = {}
    link = "https://financials.morningstar.com/ratios/r.html?t=" + stock_ms + "&culture=en&platform=sal"
    if out: print("Reading morningstar financials web data for", stock_ms, "...approx 3sec...")
    options = Options ()
    options.add_argument ('--headless')
    options.add_experimental_option ('excludeSwitches', ['enable-logging'])

    path = os.path.abspath (os.path.dirname (sys.argv[0]))
    if platform == "win32":
        cd = '/chromedriver.exe'
    elif platform == "linux":
        cd = '/chromedriver_linux'
    elif platform == "darwin":
        cd = '/chromedriver'
    driver = webdriver.Chrome (path + cd, options=options)

    driver.get (link)
    time.sleep (2)
    soup = BeautifulSoup (driver.page_source, 'html.parser')

    tmp_list = []
    for e in soup.find_all(["th","td"]):
        if len(e.text.strip()) == 0: continue
        elif e.text.strip() in ["Revenue %","Operating Income %","Net Income %","EPS %"]:
            tmp_list.append (e.text.strip ())
            for i in range(11): tmp_list.append("")
        else: tmp_list.append(e.text.strip())
    tmp_list.insert(0,"Header")

    if "Interest Coverage" not in tmp_list: return {}
    idx = tmp_list.index("Interest Coverage")
    tmp_list.insert(idx+12,"Growth")

    row = 1
    for i in range(0,len(tmp_list),12):
        key = tmp_list[i]
        if row == 2: key = "Revenue Mil bc"
        elif row == 4: key = "Operating Income Mil bc"
        elif row == 6: key = "Net Income Mil bc"
        elif row == 7: key = "Earnings Per Share"
        elif row == 8: key = "Dividends bc"
        elif row == 9: key = "Payout Ratio %"
        elif row == 11: key = "Book Value Per Share bc"
        elif row == 12: key = "Operating Cash Flow Mil bc"
        elif row == 13: key = "Cap Spending Mil bc"
        elif row == 14: key = "Free Cash Flow Mil bc"
        elif row == 15: key = "Free Cash Flow Per Share bc"
        elif row == 16: key = "Working Capital Mil bc"
        elif row in [38, 39, 40, 41]: key = "Revenue "+key+" %"
        elif row in [43, 44, 45, 46]: key = "Operating Income " + key + " %"
        elif row in [48, 49, 50, 51]: key = "Net Income " + key + " %"
        elif row in [53, 54, 55, 56]: key = "EPS " + key + " %"
        erg[key] = tmp_list[i+1:i+12]
        row += 1

    for key,val in erg.items():
        for idx,cont in enumerate(val):
            erg[key][idx] = clean_value(erg[key][idx])
        erg[key].reverse()

    return(erg)

def read_ecoCal(from_dt, to_dt, country, hl=True):
    """
    read event messages from the economic calendr on investing.com
    :param from_dt: from which date the events should be read
    :param to_dt: to which date the events should be read
    :param country: events from which country should be read
    USA country5, GBP country4, GER country17, FRA country22
    :param hl: TRUE to work in headless mode in the background - of FALSE to work on the front
    :return:
    """
    driver = rtt.define_driver(headless=hl)
    #driver = rtt.defineDriverFF (headless=hl)
    popup_close = False
    SLEEP = 3
    from_dt = datetime.strftime(from_dt, "%d/%m/%Y")
    to_dt = datetime.strftime(to_dt, "%d/%m/%Y")

    print ("Fetching data from site...")
    locale.setlocale (category=locale.LC_ALL, locale="German")
    link = "https://de.investing.com/economic-calendar/"
    #link = "https://www.investing.com/economic-calendar/"
    driver.get (link)
    time.sleep (SLEEP)

    try:
        rtt.close_popup (driver,mode="id",cont="onetrust-accept-btn-handler")
        time.sleep (SLEEP)
    except:
        pass

    # open filter
    print("Select Filter...")
    while True:
        if popup_close == False: popup_close = rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
        time.sleep (SLEEP)
        try:
            driver.find_element_by_id ("filterStateAnchor").click ()
            time.sleep (SLEEP)
            break
        except:
            "Trying again to close popup..."

    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    element = driver.find_element_by_xpath ('//*[@id="calendarFilterBox_country"]/div[1]/a[2]')
    webdriver.ActionChains (driver).move_to_element (element).click (element).perform ()
    time.sleep (SLEEP)

    print("Select Country...")
    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    driver.find_element_by_id (country).click ()
    time.sleep (SLEEP)

    """
    # select 2star importance
    print ("Select Importance...")
    while True:
        if popup_close == False: popup_close = rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
        time.sleep (SLEEP)
        try:
            driver.find_element_by_id ("importance2").click ()
            time.sleep (SLEEP)
            break
        except:
            "Trying again to close popup..."

    # select 3star importance
    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    driver.find_element_by_id ("importance3").click ()
    time.sleep (SLEEP)
    """

    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    element = driver.find_element_by_id ('ecSubmitButton')
    webdriver.ActionChains (driver).move_to_element (element).click (element).perform ()
    # driver.find_element_by_id("ecSubmitButton").click()
    time.sleep (SLEEP)

    print("Search Items From Filter...")
    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    element = driver.find_element_by_id ('datePickerToggleBtn')
    webdriver.ActionChains (driver).move_to_element (element).click (element).perform ()
    time.sleep (SLEEP)

    while True:
        if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
        try:
            driver.find_element_by_id ("startDate").clear ()
            time.sleep (SLEEP)
            break
        except:
            pass

    print("Select Start Date...")
    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    element = driver.find_element_by_id ('startDate')
    if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    # webdriver.ActionChains(driver).move_to_element(element ).send_keys ("01/01/2016")
    element.send_keys (from_dt)
    # driver.find_element_by_id("startDate").send_keys ("01/01/2016")
    time.sleep (SLEEP)

    print ("Select End Date...")
    while True:
        if popup_close == False: popup_close = rtt.close_popup (driver,mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
        try:
            driver.find_element_by_id ("endDate").clear ()
            time.sleep (SLEEP)
            break
        except:
            pass

    if popup_close == False: popup_close = rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    element = driver.find_element_by_id ('endDate')
    if popup_close == False: popup_close = rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
    # webdriver.ActionChains(driver).move_to_element(element ).send_keys ("01/01/2018")
    element.send_keys (to_dt)
    # driver.find_element_by_id("endDate").send_keys ("01/01/2018")
    time.sleep (SLEEP)

    print("Search For Data...")
    while True:
        if popup_close == False: popup_close = rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
        try:
            element = driver.find_element_by_id ('applyBtn')
            webdriver.ActionChains (driver).move_to_element (element).click (element).perform ()
            time.sleep (SLEEP)
            break
        except:
            pass

    """
    # ScrollDown Variant1
    # scroll down to very bottom for the content
    lenOfPage = driver.execute_script (
        "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
    print("LenOfPage: ",lenOfPage)    
    match = False
    sitenr = 1
    while (match == False):
        lastCount = lenOfPage
        time.sleep (SLEEP)
        try:
            rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
            time.sleep (SLEEP)
        except:
            pass
        for i in range(3): driver.find_element_by_xpath ('/html/body').send_keys (Keys.PAGE_UP)
        time.sleep (SLEEP+4)
        lenOfPage = driver.execute_script (
            "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        print ("LenOfPage: ", lenOfPage)
        print ("Reading data from site", sitenr, "...")
        sitenr += 1
        # if scrolldown // 2 == 0:
        #    soup = BeautifulSoup (driver.page_source, 'html.parser')
        #    dts = soup.find_all ("td", attrs={"class": "theDay"})
        #    print("Reading data till",dts[-1].text)
        if lastCount == lenOfPage:
            match = True
    """



    # ScrollDown Variant2
    # Get scroll height
    last_height = driver.execute_script ("return document.body.scrollHeight")
    sitenr = 1
    while True:
        #time.sleep (SLEEP)
        rtt.wait_countdown (SLEEP)
        try:
            print("Try to close PopUp...")
            rtt.close_popup (driver, mode="xpath",cont='//*[@id="PromoteSignUpPopUp"]/div[2]/i')
            # time.sleep (SLEEP)
            rtt.wait_countdown (SLEEP)
        except:
            pass

        for i in range (1):
            #driver.find_element_by_xpath ('/html/body').send_keys (Keys.PAGE_UP)
            driver.execute_script ("window.scrollTo(0, document.body.scrollHeight * 0.75)")
            print("Page Up triggered...")
        #time.sleep (SLEEP)
        rtt.wait_countdown (SLEEP)

        # Scroll down to bottom
        print("Scroll Down To Bottom...")
        driver.execute_script ("window.scrollTo(0, document.body.scrollHeight);")
        #time.sleep (SLEEP)
        rtt.wait_countdown (SLEEP)

        # Wait to load page
        print("Waiting to load...")
        #time.sleep (SLEEP+4)
        rtt.wait_countdown (SLEEP+4)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script ("return document.body.scrollHeight")
        print ("LenOfPage: ", new_height)
        print ("Reading data from site", sitenr, "...")
        sitenr += 1
        if new_height == last_height: break
        last_height = new_height

    list_cont = []
    soup = BeautifulSoup (driver.page_source, 'html.parser')
    rows = soup.find_all ("tr")

    for row in rows:
        list_row = []
        cells = row.find_all ("td")

        # append only when it is a row for date (len 1) or with all entries (len 8)
        if len (cells) in [1, 8]:
            for i_idx, i_cont in enumerate (cells):
                if i_idx == 2:
                    stars = i_cont.find_all ("i", attrs={"class": "grayFullBullishIcon"})
                    list_row.append (len (stars))
                else:
                    i_cont = i_cont.text.replace ("/xa0", "")
                    list_row.append (i_cont.strip ())

                # print (list_row)
                # print ("DEBUG LENCells, list_row", len (cells), list_row)
            if len (list_row) > 1: list_row.pop ()
            if list_row[0] in ["Offen"] or "min" in list_row[0]: continue
            list_cont.append (list_row)
    erg = {}
    erg["Header"] = ["country", "relevance", "event", "actual", "forecast", "before"]
    tmp_cont = []
    act_date = 0
    for idx, i in enumerate(list_cont):
        if len (i) == 1:
            act_date = i[0]
        else:
            tmp_dt = act_date + " " + i[0]
            # print("Debug I: ",i)
            # print(tmp_dt,len(tmp_dt),type(tmp_dt))
            dt = datetime.strptime (tmp_dt, "%A, %d. %B %Y %H:%M")
            dt = str(dt) + "#" + str(idx)
            erg[dt] = i[1:]
    driver.quit ()
    return (erg)

def read_gurufocus_data(stock,out=True,wait=1):
    """
    read gurufocus stock data
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :param wait: wait for x seconds during progress
    :return: dictionary with values per line
    """
    erg = {}

    link = "https://www.gurufocus.com/term/Owner_Earnings/" + stock + "/"
    if out: print ("Reading gurufocus data for", stock, "...")
    erg["symbol"] = stock

    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (wait)
    table = soup.find(id="def_body_detail_height")

    infoFont = table.findAll("font")
    # check if there is no data from gurufocus for the stock
    if infoFont[0].text.strip() == ": (As of . 20)": return {}

    erg["OE per Share"] = clean_value(infoFont[0].text.split(" ")[1])
    if not isinstance(erg["OE per Share"],float): return {}

    infoStrong = table.findAll("strong")
    lstStrong = []
    for i in infoStrong: lstStrong.append(i.text.strip())

    # print(f"DEBUG: {lstStrong}")

    pattern1 = re.compile ("^[a-zA-Z]{3}. [0-9]{4}$")
    for i,e in enumerate(lstStrong):
        if i > 3: break
        if pattern1.match (lstStrong[0]) != None:
            if i == 0: erg["OE per Share Date"] = clean_value(datetime.strftime(datetime.strptime (e, "%b. %Y"), "%Y-%m-%d"))
            if i == 2:
                if e == "today": erg["Price To OE Date"] = clean_value(str(datetime.today().date()))
                else: erg["Price To OE Date"] = clean_value(e)
            if i == 3: erg["Price To OE"] = clean_value(e)
        else:
            if i == 0:
                erg["Price To OE Date"] = clean_value (str (datetime.today ().date ()))                # print(f"Stock: {stock}")
                # print(f"DEBUG: {e}")
                if "Current:" in e:
                    erg["Price To OE"] = clean_value (e.split("Current:")[1].strip())
                else:
                    print(f"No valid Price To OE in Gurufocus for Stock {stock}")
                    erg["Price To OE"] = "N/A"
                infoFont = table.find ("font").text.strip()
                tmpDate = infoFont.split("(As of ")[1].replace(")","")
                erg["OE per Share Date"] = clean_value(datetime.strftime(datetime.strptime (tmpDate, "%b. %Y"), "%Y-%m-%d"))

    link = "https://www.gurufocus.com/term/grahamnumber/" + stock + "/"
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (wait)
    table = soup.find(id="def_body_detail_height")

    infoFont = table.findAll("font")
    #for i in infoFont: print(i.text)
    erg["Graham Number"] = clean_value(infoFont[0].text.split(" ")[1].replace("$",""))

    link = "https://www.gurufocus.com/term/lynchvalue/" + stock + "/"
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (wait)
    table = soup.find(id="def_body_detail_height")

    infoFont = table.findAll("font")
    #for i in infoFont: print(i.text)
    erg["Lynch Fair Value"] = clean_value(infoFont[0].text.split(" ")[1].replace("$",""))

    link = "https://www.gurufocus.com/stock/" + stock + "/summary"
    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep (wait)

    tmpDiv = soup.find ("div", {"id": "valuation"})
    lstCont = []
    tmpTD = tmpDiv.find_all ("td")
    for i in tmpTD:
        if len(i.text.strip()) > 0 and i.text.strip() != "N/A":
            lstCont.append(i.text.strip())
    lstCont.pop(0)

    for i,e in enumerate(lstCont):
        if i % 2 == 0:
            erg[e] = clean_value(lstCont[i+1])

    tmpDiv = soup.find ("div", {"id": "financial-strength"})
    lstCont = []
    tmpTD = tmpDiv.find_all ("td")
    for idx,e in enumerate(tmpTD):
        if len(e.text.strip()) > 0 and len(e.text.strip()) < 20 and "WACC" not in e.text.strip():
            if e.text.strip() != "N/A": lstCont.append(e.text.strip())
        if "ROIC" in e.text.strip() and "WACC" in e.text.strip() and "%" in e.text.strip():
            tmpElem = e.text.strip().split(" ")
            tmpROIC = tmpElem[1].split("%")[0]
            tmpWACC = tmpElem[2].replace("%","")
            lstCont.extend(["ROIC",tmpROIC,"WACC",tmpWACC])

    lstCont = lstCont[2:]
    for i,e in enumerate(lstCont):
        if i % 2 == 0:
            erg[e] = clean_value(lstCont[i+1])
            if isinstance(erg[e],str): erg[e] = "N/A"
    return(erg)


if __name__ == '__main__':
    SUMMARY = True
    PROFILE = False
    STATISTIC = False
    INCSTAT = False
    BALSHEET = False
    CASHFLOW = False
    ANALYSIS = False
    HISTPRICE = False
    HISTPRIC_DATE = False
    DAYPRICE = False
    RATING_ZACKS = False
    EARNING_CAL = False
    RATING_WSJ = False
    MORNINGSTAR = False
    HIST_DIVIDENDS = False
    HIST_SPLITS = False
    IPOS = False
    OPTIONS = False
    INVESTING_ECOCAL = False
    CLEAN_VALUE = False
    GURU_FOCUS = False

    stock = "AAPL"
    # stock = "DLR"
    #stock = "UN"
    #stock = "AMZN"
    stock_ms = "0P000000GY"  #AAPL
    stock_ms = "0P00015BGZ"

    erg = {}
    if SUMMARY: erg = read_yahoo_summary(stock,att=3)
    if PROFILE: erg = read_yahoo_profile(stock)
    if STATISTIC: ergData, ergTable = read_yahoo_statistics(stock,wait=0)
    if INCSTAT: erg = read_yahoo_income_statement(stock)
    if BALSHEET: erg = read_yahoo_balance_sheet(stock)
    if CASHFLOW: erg = read_yahoo_cashflow(stock)
    if ANALYSIS: erg = read_yahoo_analysis(stock)
    if HISTPRICE: erg = read_yahoo_histprice(stock)
    if HISTPRIC_DATE: erg = read_yahoo_histprice(stock,datetime(2020,1,1))
    if DAYPRICE: erg_price = read_dayprice (erg, "2018-12-30", "+")
    #OLDif RATING_ZACKS: erg = read_zacks_rating(stock)
    if EARNING_CAL: erg = read_yahoo_earnings_cal(stock)
    if RATING_WSJ: erg = read_wsj_rating(stock)
    if MORNINGSTAR: erg = read_morningstars_financials(stock_ms)
    if HIST_DIVIDENDS: erg = read_yahoo_histdividends(stock)
    if HIST_SPLITS: erg = read_yahoo_histsplits (stock)
    if IPOS: erg = read_ipos(read_from = datetime(2018,12,31), read_to = datetime(2017,1,1))
    if OPTIONS: erg = read_yahoo_options (stock, read_to=datetime(2020,10,2), what="Puts")
    if INVESTING_ECOCAL: erg = read_ecoCal (from_dt=datetime (2020, 5, 15), to_dt=datetime (2020, 5, 15), country="country5", hl=False)
    if GURU_FOCUS: erg = read_gurufocus_data(stock,wait=0.5)
    # USA country5, GBP country4, GER country17, FRA country22

    if STATISTIC:
        for key,val in ergData.items(): print(key,val,type(val))
        for key,val in ergTable.items(): print(key,val)
    elif IPOS:
        print(erg)
    elif CLEAN_VALUE:
        print(clean_value ("4.33B", out="None"))
        print(clean_value ("120M", out="None"))
        print(clean_value ("300T", out="None"))
        print(clean_value ("433B", out="None"))
        print(clean_value ("+-54.00%", out="None"))
        print(clean_value ("-54.00%", out="None"))
        print(clean_value ("42.749.274.398", dp=",", out="None"))
        print (clean_value ("8.652.094.026,455", dp=",", out="None"))
        print (clean_value ("180,91", dp=",", out="None"))
        print (clean_value ("112.13", out="None"))
        print (clean_value ("2,954.91", out="None"))    # stock prices from AMZN, 005930.KS Samsung)
        print (clean_value ("nan", out="None"))
        print (clean_value ("Tivoli A/S", out="None"))
        print(clean_value("6/30/2020"))
        print (clean_value ("309.76B",tcorr=True))
    else:
        for key, val in erg.items (): print (key, val, type(val))
        # print(erg, type(erg))
        # json = json.dumps(erg)
        # print(json, type(json))

    # a = np.int64(0)
    # print("Before:",a,type(a))
    # a = clean_value (a, out="None")
    # print("Before:",a,type(a))




