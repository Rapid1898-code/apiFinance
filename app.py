# Direct access to the API on Heroku
# (only working for the links where are no checks for rapidapi.com in the header))
    # https://financerapidapi.herokuapp.com/
    # https://financerapidapi.herokuapp.com/api/v1/yfSummary?ticker=CAT
    # https://financerapidapi.herokuapp.com/api/v1/yfProfile?ticker=CAT    
    # https://financerapidapi.herokuapp.com/api/v1/yfIncstatAct?ticker=CAT    
    # https://financerapidapi.herokuapp.com/api/v1/yfStatisticAct?ticker=AAPL    
    # https://financerapidapi.herokuapp.com/api/v1/yfBalSheetAct?ticker=AAPL    
    # https://financerapidapi.herokuapp.com/api/v1/yfCashFlowAct?ticker=AAPL    
    # https://financerapidapi.herokuapp.com/api/v1/yfAnalysisAct?ticker=AAPL    
    # https://financerapidapi.herokuapp.com/api/v1/yfHistPrice?ticker=AAPL    
    # https://financerapidapi.herokuapp.com/api/v1/yfHistPrice?ticker=AAPL&todate=2020-01-01    
    # https://financerapidapi.herokuapp.com/api/v1/yfDayPrice?ticker=AAPL&dt=2020-03-02    
    # https://financerapidapi.herokuapp.com/api/v1/yfHistDivs?ticker=AAPL    
    # https://financerapidapi.herokuapp.com/api/v1/dbIncstat?ticker=FB&dt=2019-12-31    
    # https://financerapidapi.herokuapp.com/api/v1/dbIncstat?ticker=FB    
    # https://financerapidapi.herokuapp.com/api/v1/levermannScore?ticker=FB 

import flask
from flask import request, jsonify
from datetime import datetime, timedelta
from datetime import date
import requests
from bs4 import BeautifulSoup
import time
import calendar
import re
import timeit
import urllib.request,urllib.error
import csv
import codecs
import yfinance
import pandas
from sqlalchemy import create_engine
from sqlalchemy.sql import text

def replace_more (inp_str, list_chars, target_char=""):
    """
    Replace several chars in a string
    :param inp_str: string which should be changed
    :param list_chars: which chars should be changed in list-form
    :param target_char: with which char the list_chars should be replaced - default is ""
    :return: changed string
    """
    for i in list_chars:
        inp_str = inp_str.replace(i,target_char)
    return inp_str

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
    value = replace_more(str(value).strip(), ["%","+-","+"])

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
    elif ("M" in value or "B" in value or "T" in value or "k" in value) and replace_more(value, [",",".","M","B","T","k","-"]).isdigit():
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
        if char in ["M", "B", "T"]: value = replace_more(value, [".",",",char])
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
    elif replace_more(value, [",",".","-"]).isdigit () == True:
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

def printNumAbbr(value):
    """
    Make abbreviaton for numeric value in thousands (K), millions (M), billions (B) or trillions (T)
    :param value: numeric value which should be abbreviated
    :return: string value with maximum possible abbreviation
    """
    minus = False
    if value in ["N","N/A",None,""]: return("N/A")
    if str(value)[0] == "-":
        value = float(str(value).replace ("-", ""))
        minus = True
    if value > 1000000000000:
        tmp = round(value / 1000000000000,2)
        if minus: return ("-"+str(tmp)+"T")
        else: return (str(tmp)+"T")
    elif value > 1000000000:
        tmp = round (value / 1000000000, 2)
        if minus: return ("-"+str(tmp)+"B")
        else: return (str (tmp) + "B")
    elif value > 1000000:
        tmp = round (value / 1000000, 2)
        if minus: return ("-"+str(tmp)+"M")
        else: return (str (tmp) + "M")
    elif value > 1000:
        tmp = round (value / 1000, 2)
        if minus: return ("-"+str(tmp)+"K")
        else: return (str (tmp) + "K")
    else: return value

def growthCalc(listElem, countElem):
    """
    Return the Growth Rate of Elements in a list
    :param listElem:    Individual elements in list (calculate growth from right value to left value)
    :param countElem:   Number of Elements for which the growth should be calculated
                        if -1 then the elements are counted automatic
    :return:            growth rate as float
    """
    initialListElem = listElem
    initialCountElem = countElem
    if countElem == -1:
        for idx in range(len(listElem)-1,-1,-1):
            if listElem[idx] in [None,"","N/A",0]:
                del listElem[idx]
        if len(listElem) == 0:
            return ("N/A")
        countElem = len(listElem) - 1
    else:
        for idx in range(len(listElem)-1,-1,-1):
            if listElem[idx] in [None,"","N/A",0]:
                del listElem[idx]
        if len(listElem) <= countElem:
            countElem = len(listElem) - 1

    listGrowth = []
    tmpGrowth = 0
    for i, e in enumerate (listElem):
        if i < len (listElem) - 1:
            try:
                tmpGrowth = (e - listElem[i + 1]) / listElem[i + 1] * 100
            except:
                print(f"Growth Calculation not possible: {e}, Content: {listElem}, ListElem: {listElem}, CountElem {countElem}, "
                      f"Initial ListElem {initialListElem}, Initial CountElem {initialCountElem}")
            listGrowth.append (tmpGrowth)
    count = 0
    sumGrowth = 0

    # print(f"DEBUG: ListGrowth: {listGrowth}")
    # print(f"DEBUG: CountElem: {countElem}")

    for i, e in enumerate (listGrowth):
        sumGrowth += e
        count += 1
        if count == countElem: break
    if count == 0 and sumGrowth == 0:
        return("N/A")
    else:
        return sumGrowth / countElem

def read_yahoo_summary(stock,out=True,att=5):
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

    # if "." in ps[1].text:
    #     if USE_PYCOUNTRY:
    #         land = ps[1].text.split(".")[-1].upper()
    #         if land == "COM":
    #             erg["country"] = "USA"
    #         else:
    #             country = pycountry.countries.get (alpha_2=land)
    #             if country != None:
    #                 erg["country"] = country.name
    #             else:
    #                 erg["country"] = "N/A"
    #     else:
    #         erg["country"] = "N/A"
    # else: erg["country"] = "N/A"

    listPElems = []
    pElems = table.find_all ("p")
    for elem in pElems[0]:
        elem = str(elem)
        listPElems.append(elem)
    for idx,elem in enumerate(listPElems):
        if ("href" in elem and "tel:" in elem) or ("http://" in elem):
            if ("href" in elem and "tel:" in elem):
                diffIDX = 3
            else:
                diffIDX = 5
            erg["country"] = listPElems[idx - diffIDX].strip()
            if erg["country"] == "United States":
                erg["country"] = "USA"
            break

    table = soup.find ('section', attrs={"class": "quote-sub-section Mt(30px)"})
    if table == None: erg["desc"] = "N/A"
    else: erg["desc"] = table.find ("p").text.strip ()

    return(erg)

def readYahooIncomeStatement(stock, out=True, calc=False, wait=1):
    """
    Read income statement stock data from yahoo (without expanding all details)
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """

    # start = timeit.default_timer ()

    erg = {"Header": stock}
    link = "https://finance.yahoo.com/quote/" + stock + "/financials?p=" + stock
    if out: print ("Reading income statement web data for", stock)

    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep(wait)

    # check if cooldown is necessary
    tmpSpans = []
    cooldown = 180
    for elem in soup.find_all ("span"):
        tmpSpans.append(elem.text)
    if "We’re sorry, we weren’t able to find any data." in tmpSpans \
        and "Please try reloading the page." in tmpSpans:
        print(f"No data for stock - probably cooldown necessary......")
        for i in range (cooldown, 0, -1):  # Delay for 30 seconds - countdown in one row
            sys.stdout.write (str (i) + ' ')  # Countdown output
            sys.stdout.flush ()
            time.sleep (1)
            if i == 1:
                print("\n")

    # read header of table
    divHeader = soup.find ("div", attrs={"class": "D(tbr) C($primaryColor)"})
    time.sleep (wait)
    tmpHeader = []

    if divHeader == None:
        print(f"No income statement data for stock {stock}...")
        return {}
    for colHeader in divHeader.find_all("span"):
        if colHeader.text in ["Header","Breakdown"]:
            continue
        else:
            tmpHeader.append(clean_value(colHeader.text))
    erg["Breakdown"] = tmpHeader

    # read content of table
    divTable = soup.find_all ("div", attrs={"data-test": "fin-row"})
    for idx, elem in enumerate(divTable):
        # print(f"DEBUG: {idx}")
        # print(f"DEBUG: {elem.prettify()}")

        # read first column
        tmpName = elem.find ("span")
        # print(f"DEBUG: {tmpName.text}")

        # read value from ttm
        tmpDiv = elem.find_all ("div", attrs={"data-test": "fin-col"})
        tmpCont = []
        for divElem in tmpDiv:
            tmpValue = divElem.find ("span")
            # print(f"DEBUG: {tmpValue}")
            if tmpValue != None:
                tmpCont.append(clean_value(tmpValue.text,tcorr=True))
            else:
                tmpCont.append(None)
        erg[tmpName.text] = tmpCont

    if calc:
        erg["Calc_EPSGrowth1Y"] = clean_value(growthCalc(erg.get("EBIT", "[]"),2))
        erg["Calc_EPSGrowthHist"] = clean_value(growthCalc(erg.get("EBIT", "[]"),-1))
        erg["Calc_RevenueGrowth1Y"] = clean_value(growthCalc(erg.get("Total Revenue", "[]"),2))
        erg["Calc_RevenueGrowthHist"] = clean_value(growthCalc(erg.get("Total Revenue", "[]"),-1))
        erg["Calc_NetIncomeGrowthHist"] = clean_value(growthCalc(erg.get("Net Income Common Stockholders", "[]"),-1))
        erg["Calc_OperatingIncomeGrowthHist"] = clean_value(growthCalc(erg.get("Operating Income", "[]"),-1))
        erg["Calc_ShareBuybacks"] = clean_value (growthCalc (erg.get ("Diluted Average Shares", "[]"), -1))
        # check if drawback for earnings in the last years
        drawback = False
        drawbackPerc = 0
        listEPS = erg.get("EBIT", "[]")
        for i,e in enumerate(listEPS):
            if i < len(listEPS) - 2 and drawback == False:
                tmpGrowth = (e - listEPS[i + 1]) / listEPS[i + 1] * 100
                if tmpGrowth < -50:
                    drawback = True
                    drawbackPerc = tmpGrowth
        if drawback == False:
            erg["Calc_EBITDrawback"] = 1
        else:
            erg["Calc_EBITDrawback"] = clean_value(drawbackPerc)

    # stop = timeit.default_timer ()
    # ic(round(stop-start,2))

    return (erg)

def read_yahoo_statistics(stock,out=True,wait=0):
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
        if idx_header > 0:
            # logic when the there is a table on the statistic site
            for i in range(0,len(tmp_list_val),idx_header):
                if tmp_list_val[i] != "":
                    if tmp_list_val[i][-1] in ["1","2","3","4","5","6"]:
                        tmp_list_val[i] = tmp_list_val[i][:len(tmp_list_val[i])-2]
                else: tmp_list_val[i] = "Header"
                erg_val[tmp_list_val[i]] = tmp_list_val[i+1:i+idx_header]
        else:
            # logic when there is no table on the statistic site
            erg_val["Header"] = ["Actual"]
            for idx, elem in enumerate(tmp_list_val):
                if idx % 2 == 0:
                    elem = elem.replace(" 5","").replace(" 3","").replace(" 1","").replace(" 6","")
                    erg_val[elem] = [tmp_list_val[idx+1]]

    # Cleanup the values finally
    if "Header" in erg_val:
        erg_val["Header"][0] = "Current"
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

def readYahooBalanceSheet (stock, out=True, calc=False):
    """
    Read actual balance sheet stock data from yahoo (without expanding details)
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :param calc: when True then also some additional values will be calculated (eg. growths)
    :return: dictionary with one line per value and dates in columns
    """

    # start = timeit.default_timer ()

    erg = {"Header": stock}
    link = "https://finance.yahoo.com/quote/" + stock + "/balance-sheet?p=" + stock
    if out: print ("Reading balance sheet web data for", stock)

    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep(1)

    # read header of table
    divHeader = soup.find ("div", attrs={"class": "D(tbr) C($primaryColor)"})
    tmpHeader = []

    if divHeader != None:
        for colHeader in divHeader.find_all("span"):
            if colHeader.text in ["Header","Breakdown"]:
                continue
            else:
                tmpHeader.append(clean_value(colHeader.text))
        erg["Breakdown"] = tmpHeader
    else:
        return{}

    # read content of table
    divTable = soup.find_all ("div", attrs={"data-test": "fin-row"})
    for idx, elem in enumerate(divTable):
        # print(f"DEBUG: {idx}")
        # print(f"DEBUG: {elem.prettify()}")

        # read first column
        tmpName = elem.find ("span")
        # print(f"DEBUG: {tmpName.text}")

        # read value from ttm
        tmpDiv = elem.find_all ("div", attrs={"data-test": "fin-col"})
        tmpCont = []
        for divElem in tmpDiv:
            tmpValue = divElem.find ("span")
            # print(f"DEBUG: {tmpValue}")
            if tmpValue != None:
                tmpCont.append(clean_value(tmpValue.text,tcorr=True))
            else:
                tmpCont.append(None)
        erg[tmpName.text] = tmpCont

    if calc:
        erg["Calc_BookValueGrowthHist"] = clean_value(growthCalc(erg.get("Tangible Book Value", "[]"),-1))

    # stop = timeit.default_timer ()
    # ic(round(stop-start,2))

    return (erg)

def readYahooCashflow (stock, out=True, calc=False):
    """
    Read cashflow stock data from yahoo (without expanding details)
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """

    # start = timeit.default_timer ()

    erg = {"Header": stock}
    link = "https://finance.yahoo.com/quote/" + stock + "/cash-flow?p=" + stock
    if out: print ("Reading cashflow web data for", stock)

    page = requests.get (link)
    soup = BeautifulSoup (page.content, "html.parser")
    time.sleep(1)

    # read header of table
    divHeader = soup.find ("div", attrs={"class": "D(tbr) C($primaryColor)"})
    tmpHeader = []
    if divHeader == None:
        print(f"No data available for stock {stock} currently...")
        return
    for colHeader in divHeader.find_all("span"):
        if colHeader.text in ["Header","Breakdown"]:
            continue
        else:
            tmpHeader.append(clean_value(colHeader.text))
    erg["Breakdown"] = tmpHeader

    # read content of table
    divTable = soup.find_all ("div", attrs={"data-test": "fin-row"})
    for idx, elem in enumerate(divTable):
        # print(f"DEBUG: {idx}")
        # print(f"DEBUG: {elem.prettify()}")

        # read first column
        tmpName = elem.find ("span")
        # print(f"DEBUG: {tmpName.text}")

        # read value from ttm
        tmpDiv = elem.find_all ("div", attrs={"data-test": "fin-col"})
        tmpCont = []
        for divElem in tmpDiv:
            tmpValue = divElem.find ("span")
            # print(f"DEBUG: {tmpValue}")
            if tmpValue != None:
                tmpCont.append(clean_value(tmpValue.text,tcorr=True))
            else:
                tmpCont.append(None)
        erg[tmpName.text] = tmpCont

    if calc:
        erg["Calc_FCFGrowthHist"] = clean_value(growthCalc(erg.get("Free Cash Flow", "[]"),-1))

    # stop = timeit.default_timer ()
    # ic(round(stop-start,2))

    return (erg)

def read_yahoo_analysis(stock, out=True, rating=False):
    """
    Read analysis stock data from yahoo
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with one line per value and dates in columns
    """

    start = timeit.default_timer ()

    erg = {}
    link = "https://finance.yahoo.com/quote/" + stock + "/analysis?p=" + stock
    if out: print("Reading analysis web data for", stock)

    attempt = 1
    table = None
    while attempt < 5 and table == None:
        try:
            page = requests.get (link)
            time.sleep (0.5)
            soup = BeautifulSoup (page.content, "html.parser")
            time.sleep (0.5)
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
    # stop = timeit.default_timer ()
    # print(f"Runtime: {round (stop - start, 2)}")

    if rating:
        options = Options ()
        options.add_argument ('--headless')
        options.add_argument ("--window-size=1920,1080")
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
        wait = WebDriverWait (driver, 2)
        WebDriverWait (driver, 5).until (EC.presence_of_element_located ((By.NAME, "agree"))).click ()
        WebDriverWait (driver, 5).until (EC.presence_of_element_located ((By.ID, "YDC-Col1")))
        element = driver.find_element_by_id ("YDC-Col1")
        driver.execute_script ("arguments[0].scrollIntoView();", element)
        time.sleep (1)
        soup = BeautifulSoup (driver.page_source, 'html.parser')
        table = soup.find (id="YDC-Col2")
        rating = table.find ("div", attrs={"data-test": "rec-rating-txt"})
        if rating not in [None,""]:
            erg["Rating"] = clean_value(rating.text)

        spans = table.find_all ("span")
        for idx, span in enumerate (spans):
            if span.text in ["Current", "Average", "Low", "High"]:
                erg["Price Target 1Y " + span.text] = clean_value(spans[idx + 1].text)

    if "Earnings Estimate" in erg or "Rating" in erg: return (erg)
    else: return ({})

def read_yahoo_histprice(stock, read_to=datetime(1950,1,1), keyString=True, out=True):
    """
    read historic stock prices
    :param stock: ticker-symbol which should be read
    :param read_to: datetime - how long in the past the prices should be read
    :param keyString: if True key-output as String in format yyyy-mm-dd - if False output as datetime
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

    # print(f"DEBUG: {link}")

    if out: print("Reading historical share price data for", stock, "...")
    try:
        ftpstream = urllib.request.urlopen(link)
    except urllib.error.URLError as e:
        print("CSV-Link can not be opened...")
        print(f"Reason: {e.reason}")
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

        if keyString or tmp_list[i][0] in ["Date"]:
            erg[tmp_list[i][0]] = tmp_list[i][1:]
        else:
            tmpKeyDateTime = datetime.strptime(tmp_list[i][0], "%Y-%m-%d")
            erg[tmpKeyDateTime] = tmp_list[i][1:]

    for key, val in erg.items ():
        for i_idx,i_cont in enumerate(val):
            erg[key][i_idx] = clean_value (i_cont,dp=".")

    return erg

def read_yFinanceRating(stock, out=True):
    """
    read rating for stocks on basis the Recommendatins from the yFinance module
    :param stock: ticker-symbol which should be read
    :param out: when True then output some status informations during program running
    :return: dictionary with informations per line and timespans in columns
    """

    start = timeit.default_timer ()

    erg = {}
    
    dataYF = yfinance.Ticker(stock)    
    tday = datetime.today()
    fromDT = str((tday - timedelta(days=360)).date())
    tday = str(tday.date())
    dfRecommendations = dataYF.recommendations

    if dfRecommendations is not None:
        pandas.set_option('display.max_rows', dfRecommendations.shape[0]+1)
        dfRecommendations = dfRecommendations.loc[fromDT:tday]
        dfRecommendations = dfRecommendations.reset_index()
        dfRecommendations = dfRecommendations.sort_values('Date', ascending=False)
        # print(dfRecommendations)
        # print(len(dfRecommendations))

        erg = dfRecommendations.set_index('Date').T.to_dict('list')					
        # for key, val in erg.items (): print (f"{key} => {val} {type(val)}")

        listFirms = []
        listGrades = [0,0,0,0,0]
        for key, val in erg.items (): 
            if val[0] not in listFirms:
                listFirms.append(val[0])
                if val[1] in ["Strong Buy"]:
                    listGrades[0] += 1
                elif val[1] in ["Buy","Market Outperform","Sector Outperform", "Outperform", "Overweight", "Positive"]:
                    listGrades[1] += 1            
                elif val[1] in ["Hold","Equal-Weight","In-Line","Market Perform","Neutral","Peer Peform","Perform","Sector Perform","Sector Weight","Mixed"]:
                    listGrades[2] += 1            
                elif val[1] in ["Underperform"]:
                    listGrades[3] += 1            
                elif val[1] in ["Sell","Underperform","Underperformer","Underweight","Negative","Reduce"]:
                    listGrades[4] += 1            
                else:
                    print(f"Error - wrong Grade Value {val[1]} in dataframe / dictionary...")

        sumFirms = sum(listGrades)
        sumGrades = 0
        for i,e in enumerate(listGrades):
            sumGrades += e * (i+1)

        # print(f"DEBUG: SumGrade: {sumGrades}")
        # print(f"DEBUG: SumFirms: {sumFirms}")
        rating = round(sumGrades / sumFirms,1)
        erg["rating"] = rating
        erg["opinions"] = sumFirms
        erg["opinions_detail"] = listGrades

    else:
        erg["rating"] = "N/A"
        erg["opinions"] = "N/A"
        erg["opinions_detail"] = "N/A"

    stop = timeit.default_timer ()
    # print(f"Time run: {round(stop-start,2)}")

    return (erg)

def read_dayprice(prices,date,direction):

    """
    read price for a specific date
    when date not available take nearest day in history from the date
    :param prices: list of prices
    :param date: date for what the price should be searched (in format 2020-12-24)
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


def outputStockIncStat (stock):
    engine = create_engine ("mysql+pymysql://rapidtec_Reader:I65faue#RR6#@nl1-ss18.a2hosting.com/rapidtec_stockdb")
    conn = engine.connect ()
    lstStockIncStat = []

    t = text ("select ticker, ultimo, "
              "total_revenue,"
              "operating_revenue,"
              "net_non_operating_interest_inc_exp,"
              "interest_inc_non_operating,"
              "interest_exp_non_operating,"
              "other_income_expense,"
              "other_non_operating_inc_exp,"
              "pretax_income,"
              "tax_provision,"
              "net_income_common_stokeholders,"
              "net_income,"
              "net_income_incl_noncontr_interests,"
              "net_income_cont_operations,"
              "diluted_NI_available_to_com_stockholders,"
              "basic_eps,"
              "diluted_eps,"
              "basic_avg_shares,"
              "diluted_avg_shares,"
              "total_operating_income,"
              "total_expenses,"
              "normalized_income,"
              "interest_income,"
              "interest_expense,"
              "net_interest_income,"
              "ebit,"
              "ebitda,"
              "reconciled_cost_of_revenue,"
              "reconciled_depreciation,"
              "normalized_ebidta,"
              "tax_rate_for_calcs,"
              "cost_of_revenue,"
              "gross_profit,"
              "operating_expense,"
              "selling_general_and_admin,"
              "research_and_development,"
              "operating_income,"
              "tax_effect_of_unusual_items "
              "from stock_income_stat where ticker = :w")
    result = conn.execute (t, w=stock)
    listTitles = \
        ["ticker",
         "ultimo",
         "total_revenue",
         "operating_revenue",
         "net_non_operating_interest_inc_exp",
         "interest_inc_non_operating",
         "interest_exp_non_operating",
         "other_income_expense",
         "other_non_operating_inc_exp",
         "pretax_income",
         "tax_provision",
         "net_income_common_stokeholders",
         "net_income",
         "net_income_incl_noncontr_interests",
         "net_income_cont_operations",
         "diluted_NI_available_to_com_stockholders",
         "basic_eps",
         "diluted_eps",
         "basic_avg_shares",
         "diluted_avg_shares",
         "total_operating_income",
         "total_expenses",
         "normalized_income",
         "interest_income",
         "interest_expense",
         "net_interest_income",
         "ebit",
         "ebitda",
         "reconciled_cost_of_revenue",
         "reconciled_depreciation",
         "normalized_ebidta",
         "tax_rate_for_calcs",
         "cost_of_revenue",
         "gross_profit",
         "operating_expense",
         "selling_general_and_admin",
         "research_and_development",
         "operating_income",
         "tax_effect_of_unusual_items"]

    result = list(result)
    if len(result[0]) != len(listTitles):
        print(f"Error - count of returned values is unequal title number - skipped...")
        return {}
    erg = {}
    for idx, row in enumerate(result):
        if idx == 0:
            for idx2, title in enumerate(listTitles):
                if title == "ticker":
                    erg["Header"] = [row[idx2]]
                else:
                    erg[title] = [row[idx2]]
        else:
            for idx2, title in enumerate(listTitles):
                if title == "ticker":
                    continue
                erg[title].append(row[idx2])
    for key, val in erg.items ():
        erg[key].reverse()
        if key == "ultimo":
            for idx, elem in enumerate(val):
                if elem not in [None,"","N/A"]:
                    val[idx] = datetime.strftime(elem, "%Y-%m-%d")
        elif key in ["Header","basic_eps","diluted_eps"]:
            pass
        else:
            for idx, elem in enumerate(val):
                if elem not in [None,"","N/A"]:
                    val[idx] = int(elem)

    return(erg)

def calcLevermannScore (stock, out=True, index=None, financeFlag=None, lastEarningsDate=None):
    inpIndex = index
    WAIT = 2
    erg = {}
    if out:
        print ("Calculating Levermann Score for", stock, "...")

    if index == None:
        if ".DE" in stock:
            index = "DAX"
        elif ".AS" in stock:
            index = "AEX25"
        elif ".AX" in stock:
            index = "ASX200"
        elif ".BR" in stock:
            index = "BEL20"
        elif ".CO" in stock:
            index = "EUROSTOXX600"
        elif ".FI" in stock:
            index = "EUROSTOXX600"
        elif ".HE" in stock:
            index = "EUROSTOXX600"
        elif ".HK" in stock:
            index = "HANGSENG"
        elif ".IR" in stock:
            index = "EUROSTOXX600"
        elif ".KS" in stock:
            index = "NIKKEI225"
        elif ".L" in stock:
            index = "FTSE100"
        elif ".LS" in stock:
            index = "EUROSTOXX600"
        elif ".MC" in stock:
            index = "IBEX35"
        elif ".MI" in stock:
            index = "EUROSTOXX600"
        elif ".OL" in stock:
            index = "EUROSTOXX600"
        elif ".PA" in stock:
            index = "CAC40"
        elif ".PR" in stock:
            index = "EUROSTOXX600"
        elif ".SR" in stock:
            index = "TASI"
        elif ".ST" in stock:
            index = "EUROSTOXX600"
        elif ".SW" in stock:
            index = "SMI"
        elif ".T" in stock:
            index = "NIKKEI225"
        elif ".TO" in stock:
            index = "TSX"
        elif ".VI" in stock:
            index = "ATX"
        elif ".VX" in stock:
            index = "SMI"
        else:
            index = "SP500"

    #5 - P/E-Ratio Actual / KGV Aktuell
    # Read summary-data
    summary = read_yahoo_summary(stock)
    if "name" not in summary:
        print(f"Error - Summary data for stock {stock} not found and stopped...")
        return {}
    else:
        if "(" in summary["name"]:
            summary["name"] = summary["name"].split("(")[0].strip()
        name = summary["name"]
        currency = summary["currency"]
        pe_ratio = summary.get("pe_ratio","N/A")

    # Read data
    time.sleep (WAIT)
    profile = read_yahoo_profile (stock)
    time.sleep (WAIT)
    bal_sheet = readYahooBalanceSheet (stock)
    time.sleep (WAIT)
    insstat = readYahooIncomeStatement (stock,calc=True)
    time.sleep (WAIT)
    stat1, stat2 = read_yahoo_statistics (stock)
    time.sleep (WAIT)

    analyst_rating = read_yFinanceRating(stock)
    # analyst_rating = read_wsj_rating(stock)
    # if analyst_rating == {}:
    #     time.sleep (WAIT)
    #     analyst_rating2 = read_yahoo_analysis(stock,rating=True)
        
    time.sleep (WAIT)
    analysis = read_yahoo_analysis(stock)
    time.sleep (WAIT)
    dates_earnings = read_yahoo_earnings_cal (stock)

    #0 - Common Data
    if profile != None:
        sector = profile.get("sector", "N/A")
        industry = profile.get("industry","N/A")
        empl = profile.get("empl","N/A")
    else:
        sector = "N/A"
        industry = "N/A"
        empl = "N/A"

    if financeFlag == None:
        if sector in ["Financial Services"]:
            financeFlag = "Y"
        else:
            financeFlag = "N"

    #2 - EBIT-Margin / EBIT Marge
    insstat.get ("Total Revenue", ["N/A"])
    if "EBIT" not in insstat or financeFlag.upper() == "J":
        ebit = "N/A"
        ebit_marge = "N/A"
    else:
        ebit = insstat.get ("EBIT", None)[0]
        revenue = insstat.get ("Total Revenue", None)[0]
        if ebit != None and revenue not in [None,0]: ebit_marge = round(ebit / revenue * 100,2)
        else: ebit_marge = "N/A"

    #1 - Return On Equity RoE / Eigenkapitalrendite
    tmpNetIncStock = "Not needed for calc"
    tmpCommonStockEqui = "Not needed for calc"
    if "Return on Equity (ttm)" in stat1 and stat1["Return on Equity (ttm)"] != None:
        roe = stat1["Return on Equity (ttm)"]
    else:
        tmpNetIncStock = insstat.get("Net Income Common Stockholders",None)[0]
        tmpCommonStockEqui = bal_sheet.get("Common Stock Equity",None)[0]
        if tmpNetIncStock != None and tmpCommonStockEqui != None:
            roe = round((tmpNetIncStock / tmpCommonStockEqui) * 100,2)
        else:
            roe = "N/A"

    if "Market Cap (intraday)" not in stat2: stat2["Market Cap (intraday)"] = [None]
    marketcap = stat2["Market Cap (intraday)"][0]
    if marketcap == None: marketcap = summary["marketcap"]
    if marketcap in [None,"N/A"]: cap = "N/A"
    else:
        if marketcap < 200000:
            cap = "SmallCap"
            if inpIndex == None and index == "DAX": ind = "SDAX"
        elif marketcap < 5000000:
            cap = "MidCap"
            if inpIndex == None and index == "DAX": ind = "MDAX"
        else: cap = "LargeCap"
    shares_outstanding = stat1.get("Shares Outstanding","N/A")

    if shares_outstanding in ["N/A",None,""]:
        if "Basic Average Shares" in insstat:
            for idx_so, cont_so in enumerate(insstat["Basic Average Shares"]):
                if cont_so != "N/A": break
            shares_outstanding = insstat["Basic Average Shares"][idx_so]
    hist_price_stock = read_yahoo_histprice(stock)

    if index != "TASI": hist_price_index = read_yahoo_histprice(index)
    else:
        today = datetime.today ()
        yback = timedelta (days=375)
        hist_price_index = read_yahoo_histprice (index,today - yback)

    #4 - P/E-Ratio History 5Y / KGV Historisch 5J
    if "Net Income from Continuing & Discontinued Operation" not in insstat: net_income = "N/A"
    if "Breakdown" not in insstat:
        insstat["Breakdown"] = ["N/A"]
    else:
        net_income = insstat.get("Net Income from Continuing & Discontinued Operation")
    count = eps_hist = 0
    net_income_list = []
    net_income_date_list = []
    pe_ratio_hist_list = []
    pe_ratio_hist_dates = []
    if insstat["Breakdown"][0] not in [None,"N/A"] and shares_outstanding not in [None,"N/A"] and net_income not in [None,"N/A"]:
        for idx,cont in enumerate(net_income):
            if cont == "-": continue
            else:
                if insstat["Breakdown"][idx].upper() == "TTM":
                    dt1 = datetime.strftime (datetime.today (), "%Y-%m-%d")
                    tmp_date, tmp_price = read_dayprice (hist_price_stock, dt1, "-")
                    eps_hist += tmp_price / (cont / shares_outstanding)
                    pe_ratio_hist_list.append (str (round (tmp_price / (cont / shares_outstanding), 2)))
                    pe_ratio_hist_dates.append("ttm")
                    net_income_list.append(str(printNumAbbr(cont)))
                    net_income_date_list.append("ttm")
                else:
                    if cont in [None,"N/A",""]:
                        continue
                    tmp_date, tmp_price = read_dayprice(hist_price_stock,insstat["Breakdown"][idx],"-")
                    eps_hist += tmp_price / (cont / shares_outstanding)
                    pe_ratio_hist_list.append(str(round(tmp_price / (cont / shares_outstanding),2)))
                    pe_ratio_hist_dates.append(insstat["Breakdown"][idx])
                    net_income_list.append(str(printNumAbbr(cont)))
                    net_income_date_list.append(insstat["Breakdown"][idx])
                count += 1

        pe_ratio_hist = round(eps_hist / count,2)
    else: pe_ratio_hist = "N/A"

    #3 - Equity Ratio / Eigenkaptialquote
    if "Common Stock Equity" not in bal_sheet: bal_sheet["Common Stock Equity"] = ["N/A"]
    if "Breakdown" not in bal_sheet: bal_sheet["Breakdown"] = ["N/A"]
    if bal_sheet["Common Stock Equity"][0] not in ["N/A",None] and bal_sheet["Total Assets"][0] not in ["N/A",None,0]:
        equity = bal_sheet["Common Stock Equity"][0]
        total_assets = bal_sheet["Total Assets"][0]
        eq_ratio = round(equity / total_assets * 100,2)
    else:
        equity = total_assets = eq_ratio = "N/A"

    #6 - Analyst Opinions / Analystenmeinung
    rating = analyst_rating.get("rating","N/A")
    rating_count = analyst_rating.get("opinions","N/A")    
    # if analyst_rating.get("Rating","N/A") != "N/A":
    #     rating = analyst_rating["Rating"][0]
    #     rating_count = analyst_rating["Rating"][2]
    # elif analyst_rating2.get("Rating","N/A") != "N/A":
    #     rating = analyst_rating2["Rating"]
    #     rating_count = analyst_rating2["No. of Analysts"][0]
    # else:
    #     rating = "N/A"

    #7 Reaction to quarter numbers / Reaktion auf Quartalszahlen
    last_earningsinfo = key = "N/A"
    if lastEarningsDate != None or dates_earnings != {}:
        if lastEarningsDate != None:
            key = datetime.strftime(lastEarningsDate, "%Y-%m-%d")
        else:
            for key in sorted(dates_earnings.keys(), reverse=True):
                if "Header" in key:
                    continue
                if datetime.strptime(key,"%Y-%m-%d") < datetime.today():
                    break
        last_earningsinfo = key

        stock_price_before = read_dayprice (hist_price_stock, key, "+")
        stock_price_before[1] = round (stock_price_before[1], 2)
        dt1 = datetime.strptime (stock_price_before[0], "%Y-%m-%d") + timedelta (days=1)
        dt2 = datetime.strftime (dt1, "%Y-%m-%d")
        stock_price_after = read_dayprice (hist_price_stock, dt2, "+")
        stock_price_after[1] = round (stock_price_after[1], 2)

        index_price_before = read_dayprice (hist_price_index, stock_price_before[0], "+")
        index_price_before[1] = round (index_price_before[1], 2)
        index_price_after = read_dayprice (hist_price_index, dt2, "+")
        index_price_after[1] = round (index_price_after[1], 2)
        stock_reaction = round (((stock_price_after[1] - stock_price_before[1]) / stock_price_before[1]) * 100,
                                2)
        index_reaction = round (((index_price_after[1] - index_price_before[1]) / index_price_before[1]) * 100,
                                2)
        reaction = round (stock_reaction - index_reaction, 2)
    else:
        reaction = stock_reaction = stock_price_before = stock_price_after = "N/A"
        index_reaction = index_price_before = index_price_after = "N/A"
        key = "N/A"

    #8 Profit Revision / Gewinnrevision
    #13 - Profit Growth / Gewinnwachstum
    profitGrowthCalc = False
    if analysis != {}:
        next_year_est_current = analysis["Current Estimate"][3]
        next_year_est_90d_ago = analysis["90 Days Ago"][3]

        if next_year_est_current not in [None,"N/A"] and next_year_est_90d_ago not in [None,"N/A"]:
            profit_revision = round(((next_year_est_current-next_year_est_90d_ago)/next_year_est_90d_ago)*100,2)
        else: profit_revision = "N/A"
        profit_growth_act = analysis["Current Estimate"][2]
        profit_growth_fut = analysis["Current Estimate"][3]

        if profit_growth_act in [None,"N/A"] or profit_growth_fut in [None,"N/A"]:
            profit_growth = "N/A"
        else:
            profit_growth = round(((profit_growth_fut - profit_growth_act) / profit_growth_act)*100,2)
    else:
        profit_revision = "N/A"
        profit_growth = "N/A"
        profit_growth_act = "N/A"
        profit_growth_fut = "N/A"
        next_year_est_current = "N/A"
        next_year_est_90d_ago = "N/A"
        analysis["EPS Trend"] = ["N/A","N/A","N/A","N/A"]
    if profit_growth == "N/A":
        profit_growth = round(insstat.get("Calc_EPSGrowthHist","N/A"),2)
        profitGrowthCalc = True

    #9 Price Change 6month / Kurs Heute vs. Kurs vor 6M
    #10 Price Change 12month / Kurs Heute vs. Kurs vo 1J
    #11 Price Momentum / Kursmomentum Steigend
    dt1 = datetime.strftime (datetime.today(), "%Y-%m-%d")
    dt2 = datetime.today() - timedelta (days=180)
    dt2 = datetime.strftime(dt2, "%Y-%m-%d")
    dt3 = datetime.today() - timedelta (days=360)
    dt3 = datetime.strftime(dt3, "%Y-%m-%d")
    price_today = read_dayprice(hist_price_stock,dt1,"-")
    price_6m_ago = read_dayprice(hist_price_stock,dt2,"+")
    price_1y_ago = read_dayprice(hist_price_stock,dt3,"+")
    change_price_6m = round(((price_today[1]-price_6m_ago[1]) / price_6m_ago[1])*100,2)
    change_price_1y = round(((price_today[1]-price_1y_ago[1]) / price_1y_ago[1])*100,2)

    #12 Dreimonatsreversal
    dt_today = datetime.today()
    m = dt_today.month
    y = dt_today.year
    d = dt_today.day
    dates = []
    for i in range(4):
        m -= 1
        if m == 0:
            y -= 1
            m = 12
        ultimo = calendar.monthrange(y,m)[1]
        dates.append(datetime.strftime(date(y,m,ultimo), "%Y-%m-%d"))
    stock_price = []
    index_price = []
    for i in dates:
        pr1 = read_dayprice(hist_price_stock, i, "-")
        stock_price.append([pr1[0],round(pr1[1],2)])
        pr2 = read_dayprice(hist_price_index, i, "-")
        index_price.append([pr2[0],round(pr2[1],2)])

    stock_change = []
    index_change = []
    for i in range (3, 0, -1):
        if stock_price[i - 1][1] != 0 and index_price[i - 1][1] != 0:
            stock_change.append (round (((stock_price[i][1] - stock_price[i - 1][1]) / stock_price[i - 1][1]) * 100, 2))
            index_change.append (round (((index_price[i][1] - index_price[i - 1][1]) / index_price[i - 1][1]) * 100, 2))
    if stock_change == []:
        stock_change = ["N/A", "N/A", "N/A"]
    if index_change == []:
        index_change = ["N/A", "N/A", "N/A"]

    lm_points = 0
    lm_pointsDict = {}
    #1 - check RoE
    if roe == "N/A": lm_pointsDict["roe"] = 0
    elif roe > 20: lm_pointsDict["roe"] = 1
    elif roe < 10: lm_pointsDict["roe"] = -1
    else: lm_pointsDict["roe"] = 0

    #2 - check ebit_marge
    if financeFlag in ["J","Y"] or ebit_marge == "N/A": lm_pointsDict["ebit_marge"] = 0
    else:
        if ebit_marge > 12 and financeFlag.upper() == "N": lm_pointsDict["ebit_marge"] = 1
        elif ebit_marge < 6 and financeFlag.upper() == "N": lm_pointsDict["ebit_marge"] = -1
        else: lm_pointsDict["ebit_marge"] = 0

    #3 - check eq-ratio
    if eq_ratio == "N/A": lm_pointsDict["eq_ratio"] = 0
    else:
        if financeFlag in ["J","Y"]:
            if eq_ratio > 10: lm_pointsDict["eq_ratio"] = 1
            elif eq_ratio < 5: lm_pointsDict["eq_ratio"] = -1
            else: lm_pointsDict["eq_ratio"] = 0
        else:
            if eq_ratio > 25: lm_pointsDict["eq_ratio"] = 1
            elif eq_ratio < 15: lm_pointsDict["eq_ratio"] = -1
            else: lm_pointsDict["eq_ratio"] = 0

    #4 - check pe-ratio
    if pe_ratio == "N/A": lm_pointsDict["pe_ratio"] = 0
    else:
        if pe_ratio <12 and pe_ratio > 0: lm_pointsDict["pe_ratio"] = 1
        elif pe_ratio >16 or pe_ratio <0: lm_pointsDict["pe_ratio"] = -1
        else: lm_pointsDict["pe_ratio"] = 0

    #5 - check pe-ratio history
    if pe_ratio_hist == "N/A": lm_pointsDict["pe_ratio_hist"] = 0
    else:
        if pe_ratio_hist <12 and pe_ratio_hist > 0: lm_pointsDict["pe_ratio_hist"] = 1
        elif pe_ratio_hist >16 or pe_ratio_hist <0: lm_pointsDict["pe_ratio_hist"] = -1
        else: lm_pointsDict["pe_ratio_hist"] = 0

    #6 - check rating
    if cap == "SmallCap":
        if rating == "N/A": lm_pointsDict["rating"] = 0
        elif rating_count >= 5 and rating <= 2: lm_pointsDict["rating"] = -1
        elif rating_count >= 5 and rating >= 4: lm_pointsDict["rating"] = 1
        elif rating_count < 5 and rating <= 2: lm_pointsDict["rating"] = -1
        elif rating_count < 5 and rating >= 4: lm_pointsDict["rating"] = -1
        else: lm_pointsDict["rating"] = 0
    else:
        if rating == "N/A": lm_pointsDict["rating"] = 0
        elif rating >= 4: lm_pointsDict["rating"] = 1
        elif rating <= 2: lm_pointsDict["rating"] = -1
        else: lm_pointsDict["rating"] = 0

    #7 - check to quarter numbers
    if reaction == "N/A": lm_pointsDict["reaction"] = 0
    else:
        if reaction >1: lm_pointsDict["reaction"] = 1
        elif reaction <-1: lm_pointsDict["reaction"] = -1
        else: lm_pointsDict["reaction"] = 0

    #8 - check profit revision
    if profit_revision == "N/A": lm_pointsDict["profit_revision"] = 0
    else:
        if profit_revision >1: lm_pointsDict["profit_revision"] = 1
        elif profit_revision <-1: lm_pointsDict["profit_revision"] = -1
        else: lm_pointsDict["profit_revision"] = 0

    #9 - change price 6 month
    if change_price_6m >5: lm_pointsDict["change_price_6m"] = 1
    elif change_price_6m <-5: lm_pointsDict["change_price_6m"] = -1
    else: lm_pointsDict["change_price_6m"] = 0

    #10 - change price 1 year
    if change_price_1y >5: lm_pointsDict["change_price_1y"] = 1
    elif change_price_1y <-5: lm_pointsDict["change_price_1y"] = -1
    else: lm_pointsDict["change_price_1y"] = 0

    #11 - price momentum
    if lm_pointsDict["change_price_6m"] == 1 and lm_pointsDict["change_price_1y"] in [0,-1]:
        lm_pointsDict["price_momentum"] = 1
    elif lm_pointsDict["change_price_6m"] == -1 and lm_pointsDict["change_price_1y"] in [0,1]:
        lm_pointsDict["price_momentum"] = -1
    else: lm_pointsDict["price_momentum"] = 0

    #12 month reversal effect
    if cap == "LargeCap":
        if stock_change[2]<index_change[2] and stock_change[1]<index_change[1] and stock_change[0]<index_change[0]:
            lm_pointsDict["3monatsreversal"] = 1
        elif stock_change[2]>index_change[2] and stock_change[1]>index_change[1] and stock_change[0]>index_change[0]:
            lm_pointsDict["3monatsreversal"] = -1
        else: lm_pointsDict["3monatsreversal"] = 0
    else:
        lm_pointsDict["3monatsreversal"] = 0

    ls_vs = []
    if stock_change != [] and index_change != []:
        for i in range (2, -1, -1):
            if stock_change[i] > index_change[i]:
                ls_vs.append (">")
            elif stock_change[i] < index_change[i]:
                ls_vs.append ("<")
            else:
                ls_vs.append ("=")
    else:
        ls_vs = ["N/A", "N/A", "N/A"]

    #13 - profit growth
    if profit_growth == "N/A": lm_pointsDict["profit_growth"] = 0
    else:
        if profit_growth >5: lm_pointsDict["profit_growth"] = 1
        elif profit_growth <-5: lm_pointsDict["profit_growth"] = -1
        else: lm_pointsDict["profit_growth"] = 0

    # print format marketcap
    print_cap = printNumAbbr(marketcap)

    # overall recommendation levermann full
    lm_sum = 0
    for val in lm_pointsDict.values(): lm_sum += val
    if cap in ["SmallCap", "MidCap"]:
        if lm_sum >= 7:
            rec = "Possible Buy"
        elif lm_sum in [5, 6]:
            rec = "Possible Holding"
        else:
            rec = "Possible Sell"
    else:
        if lm_sum >= 4:
            rec = "Possible Buy"
        elif lm_sum in [3]:
            rec = "Possible Holding"
        else:
            rec = "Possible Sell"

    # overall recommendation levermann light
    lm_sum_light = lm_pointsDict["roe"] + lm_pointsDict["ebit_marge"] + lm_pointsDict["pe_ratio"] \
                   + lm_pointsDict["reaction"] + lm_pointsDict["change_price_6m"]
    # overall recomendation levermann full
    if cap in ["SmallCap","MidCap"]:
        if lm_sum_light >=4: rec_light = "Possible Buy"
        else: rec_light = "Possible Sell"
    else:
        if lm_sum_light >=3: rec_light = "Possible Buy"
        else: rec_light = "Possible Sell"


    erg["name"] = summary["name"]
    erg["ticker"] = stock
    erg["index"] = index
    erg["marketCap"] = print_cap
    erg["currency"] = summary["currency"]
    erg["sector"] = sector
    erg["industry"] = industry
    erg = {**erg, **lm_pointsDict}
    erg["LastEarnings"] = clean_value(last_earningsinfo)
    erg["1roe"] = roe
    erg["2ebitMarge"] = ebit_marge
    erg["3eqRatio"] = eq_ratio
    erg["4peRatioHist"] = pe_ratio_hist
    erg["5peRatio"] = pe_ratio
    erg["6analystRating"] = rating
    erg["7quartalReaction"] = reaction
    erg["8profitRevision"] = profit_revision
    erg["9priceToday6M"] = change_price_6m
    erg["10priceToday1Y"] = change_price_1y
    erg["13profitGrowth"] = profit_growth
    erg["Levermann Score Full"] = lm_sum
    erg["Levermann Score Light"] = lm_sum_light
    erg["Recommendation Full"] = rec
    erg["Recommendation Light"] = rec_light
    erg["Cap"] = cap
    erg["Finanzwert"] = financeFlag

    return erg


app = flask.Flask(__name__)
app.config["DEBUG"] = True

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

@app.route('/', methods=['GET'])
def home():
    return "api for getting finance data fast and reliable from many sources"

@app.errorhandler(404)
def page_not_found(e):
    return "404. The resource could not be found.", 404

@app.route('/api/v1/yfSummary', methods=['GET'])
def api_yfSummary():
    # http://127.0.0.1:5000/api/v1/yfSummary?ticker=CAT
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # print("HEADER START")
    # print(request.headers)
    # print("HEADER END")
    # print(len(request.headers))
    # print(type(request.headers))

    # # check if api is called from rapidapi.com
    # if request.headers.get("Host") != "127.0.0.1:5000":
    #     if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
    #         return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    # check if needed parameter is provided
    if "ticker" in request.args:
        summary = read_yahoo_summary(ticker, att=3)
        # print(f"DEBUG: {summary}")
        if summary != {}:
            return jsonify (summary)
        else:
            return f"No summary data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfProfile', methods=['GET'])
def api_yfProfile():
    # http://127.0.0.1:5000/api/v1/yfProfile?ticker=CAT
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        profile = read_yahoo_profile(ticker)
        print(f"DEBUG: {profile}")
        print(f"DEBUG: {type(profile)}")
        if profile != {}:
            return jsonify (profile)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfIncstatAct', methods=['GET'])
def api_yfIncstatAct():
    # http://127.0.0.1:5000/api/v1/yfIncstatAct?ticker=CAT
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        incStatAct = readYahooIncomeStatement(stock=ticker)
        print(f"DEBUG: {incStatAct}")
        print(f"DEBUG: {type(incStatAct)}")
        if incStatAct != {}:
            return jsonify (incStatAct)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfStatisticAct', methods=['GET'])
def api_yfStatisticAct():
    # http://127.0.0.1:5000/api/v1/yfStatisticAct?ticker=AAPL
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        statData, statTable = read_yahoo_statistics(ticker)
        for key, val in statTable.items ():
            statData[key] = val[0]
        if statData != {}:
            return jsonify (statData)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfBalSheetAct', methods=['GET'])
def api_yfBalSheetAct():
    # http://127.0.0.1:5000/api/v1/yfBalSheetAct?ticker=AAPL
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        balSheetData = readYahooBalanceSheet (ticker)
        if balSheetData != {}:
            return jsonify (balSheetData)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfCashFlowAct', methods=['GET'])
def api_yfCashFlowAct():
    # http://127.0.0.1:5000/api/v1/yfCashFlowAct?ticker=AAPL
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        cashFlowData = readYahooCashflow (ticker)
        if cashFlowData != {}:
            return jsonify (cashFlowData)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfAnalysisAct', methods=['GET'])
def api_yfAnalysisAct():
    # http://127.0.0.1:5000/api/v1/yfAnalysisAct?ticker=AAPL
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        analysisData = read_yahoo_analysis(ticker)
        if analysisData != {}:
            return jsonify (analysisData)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfHistPrice', methods=['GET'])
def api_yfHistPrice():
    # http://127.0.0.1:5000/api/v1/yfHistPrice?ticker=AAPL
    # http://127.0.0.1:5000/api/v1/yfHistPrice?ticker=AAPL&todate=2020-01-01
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')
    dt = request.args.get ("todate")

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        if "todate" in request.args:
            tmpYear = int(dt[:4])
            tmpMonth = int(dt[5:7])
            tmpDay = int(dt[8:])
            histPriceData = read_yahoo_histprice (ticker,datetime(tmpYear,tmpMonth,tmpDay))
        else:
            histPriceData = read_yahoo_histprice(ticker)

        if histPriceData != {}:
            return jsonify (histPriceData)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfDayPrice', methods=['GET'])
def api_yfDayPrice():
    # http://127.0.0.1:5000/api/v1/yfDayPrice?ticker=AAPL&dt=2020-03-02
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')
    dt = request.args.get ("dt")

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        if "dt" in request.args:
            tmpDateTo = datetime.strftime(datetime.strptime (dt, "%Y-%m-%d") - timedelta(days=7), "%Y-%m-%d")
            tmpYear = int(tmpDateTo[:4])
            tmpMonth = int(tmpDateTo[5:7])
            tmpDay = int(tmpDateTo[8:])
            histPriceData = read_yahoo_histprice (ticker, datetime (tmpYear, tmpMonth, tmpDay))
            dayPrice = read_dayprice(histPriceData, dt, "+")
        else:
            return f"No date provided!"
        if dayPrice != {}:
            return jsonify (dayPrice)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route('/api/v1/yfHistDivs', methods=['GET'])
def api_yfHistDivs():
    # http://127.0.0.1:5000/api/v1/yfHistDivs?ticker=AAPL
    query_parameters = request.args
    ticker = query_parameters.get ('ticker')
    dt = request.args.get ("todate")

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        histDividendsData = read_yahoo_histdividends(ticker)
        if histDividendsData != {}:
            return jsonify (histDividendsData)
        else:
            return f"No profile data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"


@app.route ('/api/v1/dbIncstat', methods=['GET'])
def api_dbIncStat():
    # Output for specific date from db
        # http://127.0.0.1:5000/api/v1/dbIncstat?ticker=FB&dt=2019-12-31
    # Output everyting from db
        # http://127.0.0.1:5000/api/v1/dbIncstat?ticker=FB
    # local db with maria-db
    # hosted db on a2hosting with read permission only
    # engine = create_engine ("mysql+pymysql://rapidtec_Reader:I65faue#RR6#@nl1-ss18.a2hosting.com/rapidtec_stockdb")
    # conn = engine.connect ()

    ticker = request.args.get("ticker")
    dt = request.args.get ("dt")
    print(f"DEBUG: {ticker}")
    print(f"DEBUG: {dt}")

    # check if api is called from rapidapi.com
    if request.headers.get("Host") != "127.0.0.1:5000":
        if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
            return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if "ticker" in request.args:
        erg = outputStockIncStat(ticker)
        # print(f"DEBUG: {erg}")
        # Data with filtering on specific date
        if "dt" in request.args:
            if dt in erg["ultimo"]:
                idxDT = erg["ultimo"].index(dt)
                ergNew = {}
                ergNew["Ticker"] = erg["Header"][0]
                for key, val in erg.items ():
                    if key == "Header":
                        continue
                    ergNew[key] = erg[key][idxDT]
                print(ergNew)
                print (ergNew["operating_revenue"])
                return jsonify (ergNew)
        # Whole data for the ticker
        if erg != {}:
            print(erg)
            print(erg["operating_revenue"])
            return jsonify (erg)
        else:
            return f"No data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

@app.route ('/api/v1/levermannScore', methods=['GET'])
def api_levermannScore():
    # http://127.0.0.1:5000/api/v1/levermannScore?ticker=AAPL
    # http://127.0.0.1:5000/api/v1/levermannScore?ticker=AAPL&index=DAX
    # http://127.0.0.1:5000/api/v1/levermannScore?ticker=AAPL&financeflag=Y  
    # http://127.0.0.1:5000/api/v1/levermannScore?ticker=AAPL&lastearningsdate=2020-12-31   
    # local db with maria-db
    # hosted db on a2hosting with read permission only
    # engine = create_engine ("mysql+pymysql://rapidtec_Reader:I65faue#RR6#@nl1-ss18.a2hosting.com/rapidtec_stockdb")
    # conn = engine.connect ()

    ticker = request.args.get("ticker")
    index = request.args.get("index")
    financeFlag = request.args.get ("financeflag")
    lastEarningsDate = request.args.get ("lastearningsdate")
    print(f"DEBUG Ticker: {ticker}")
    print(f"DEBUG Index: {index}")
    print(f"DEBUG FinanceFlag: {financeFlag}")
    print(f"DEBUG LastEarningsDate: {lastEarningsDate}")    

    # # check if api is called from rapidapi.com
    # if request.headers.get("Host") != "127.0.0.1:5000":
    #     if request.headers.get("X-RapidAPI-Proxy-Secret") != "ec2f3b60-94b2-11eb-8612-4715b760a3a5":
    #         return "Error: Wrong API-Call - use www.rapidapi.com for calling the API!"

    if lastEarningsDate != None:
        lastEarningsDate = datetime.strptime(lastEarningsDate, "%Y-%m-%d")

    if "ticker" in request.args:
        erg = calcLevermannScore(ticker, index=index, financeFlag=financeFlag, lastEarningsDate=lastEarningsDate)
        # print(f"DEBUG: {erg}")
        # Data with filtering on specific date

        if erg != {}:
            print(erg)
            return jsonify (erg)
        else:
            return f"No data found for ticker {ticker}"
    else:
        return "Error: No ticker provided!"

# We only need this for local development.
if __name__ == '__main__':
 app.run()