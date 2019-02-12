# =============================================================================
#                           Financial_calednar_fx678.py
# V.1: original version by Elvis
# =============================================================================
import datetime
#from urllib import urlencode
from bs4 import BeautifulSoup
import os
import requests
import pandas as pd
import mysql.connector as sql
import configparser
import re
#from datetime import timedelta
start_date = datetime.datetime.now().strftime("%Y%m%d")
end_date = datetime.datetime.now().strftime("%Y%m%d")
daterange = pd.date_range(start_date, end_date)
#Set working directory
path = 'C:\Python27\Scripts\AutoeCalendar_prod'
os.chdir(path)
date_time = datetime.datetime.now().replace(microsecond=0)
date = date_time.date()
#date = date_time.date().strftime("%Y%m%d")
time = date_time.time()
delta = datetime.timedelta(days=1)
#Connect with Market Watch CMS environment
# =============================================================================
config = configparser.ConfigParser()
config.read("config.ini")
host = config.get('DEFAULT','host')
user = config.get('DEFAULT','user')
passwd = config.get('DEFAULT','passwd')
database = config.get('DEFAULT','database')
 
db = sql.connect(host= host, 
           user= user, 
           passwd= passwd, 
           db= database)
ecal_tbl = pd.read_sql("SELECT ID, lang, level, country, actual, forecast, previous, announce_time, announce_hour, indicator_name, data_type, revise_previous, update_status from ecalendar_tbl where actual is NULL and announce_time = '%s'" % (date),
                       con = db)
#ecal_tbl = pd.read_sql("SELECT ID, lang, level, country, actual, forecast, previous, announce_time, announce_hour, indicator_name, data_type, revise_previous, update_status from ecalendar_tbl where announce_time = '%s'" % (date),
#                       con = db)
# =============================================================================
xls_file = pd.ExcelFile('C:\Python27\Scripts\AutoeCalendar_prod\FX678_source\FX678_source_tbl.xlsx')
fx678_tbl = xls_file.parse('fx678_source_tbl')
fx678_tbl.href = fx678_tbl.href.astype(str)
fx678_tbl.loc[:, 'data_type'] = fx678_tbl.loc[:,'data_type'].fillna('')

ecal_tbl = pd.merge(ecal_tbl,
                    fx678_tbl,
                    how = 'left',
                    on = ['country', 'indicator_name', 'data_type'])

# =============================================================================
# Determine the data can be updated by robot
temp = ecal_tbl.loc[ecal_tbl.href.notnull()].ID.drop_duplicates()
cursor = db.cursor()
for i in temp:
    sql_update = "UPDATE ecalendar_tbl SET update_status = 'A' WHERE ID='%s'" % (i)
    cursor.execute(sql_update)
        
cursor.close()
# =============================================================================

def request_ecalendar_data(date):
    headers = {'Content-Type': 'text/html; charset = utf-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
    return requests.get(
        'https://rl.fx678.com/date/%s.html' % (date),
        headers = headers
    ).content.decode("utf-8", 'ignore')

def parse_html(content):
    soup = BeautifulSoup(content, 'html.parser')
    titles = []
    href = []
    public_time = []
    actual_price = []
    temp = []
    id = []
    for td in soup.find("table", {"id" : "current_data"}).findAll(["td", {'class':lambda L: L and L.startswith('tab_time tab_time')},'a', {'class': 'nowrap'}]):
        a_element = td.string
        if a_element is not None:
            titles.append(a_element.string)
    titles = filter(None, titles)
    titles = [x.replace('\n', '').strip() for x in titles]
    df = pd.DataFrame({'titles':titles})
    for i in df.index:
        df.loc[i,'cn_flag'] =len(re.findall(ur'[\u4e00-\u9fcc]+', df.loc[i,'titles']))
    df = df[(df.titles != u'\u9ad8')]
    df = df[(df.titles !=u'\u67e5\u770b\u5b8c\u6574\u4ea4\u6613\u65e5\u6570\u636e')]
    df = df[(df.titles.str.contains(":")) |(df.cn_flag >= 1) ][['titles']].reset_index(drop = True)
        
    df.loc[:, 'public_time'] = None
    for i in df.index:
        if(":" in df.iloc[i,0]):
            public_time = df.iloc[i,0]
        df.iloc[i,1] = public_time
    df = df[~df['titles'].str.contains(':')]
    
    for td in soup.find("table", {"id" : "current_data"}).findAll("td", {'class':'gb loading-bg '}):
        a_element = td.string
        b_element = td['id']
        if a_element is not None:
            actual_price.append(a_element.string)
            id.append(b_element)
    actual_price = filter(None, actual_price)
    actual_price = [x.replace('\n', '').strip() for x in actual_price]
    today_price_tbl = pd.DataFrame({'href':id, 'actual_price':actual_price})
    
    for td in soup.find("table", {"id" : "current_data"}).findAll('a', {'class': 'nowrap'}):
        a_element = td
        b_element = a_element['href'][4:-5]
        if a_element is not None:
            temp.append(a_element.string)
            href.append(b_element)
    df.loc[:, 'href'] = href
    df = df[['public_time', 'titles', 'href']].reset_index(drop = True)
    for i in df.index:
        b = "0123456789"
        c = u"\u6708\u622a\u81f3\u65e5"
        temp = ''
        for char in b:
            df.loc[i,'titles'] = df.loc[i,'titles'].replace(char, "").split('(')[0]
        for char in c:
            temp = df.loc[i,'titles'][2:].replace(char, "")
            df.loc[i,'titles'] = df.loc[i,'titles'][:2] + temp
    df = pd.merge(df, 
                  today_price_tbl,
                  how = 'left',
                  on = 'href')
    
    return df

def aggregate_data(daterange):
#    date = date_time.date().strftime("%Y%m%d")
    data = pd.DataFrame()
    for single_date in daterange:
        date = single_date.strftime("%Y%m%d")
        content = request_ecalendar_data(date)
        try:
            df = parse_html(content)
        except:
            pass
        data = data.append(df)
        data = data.drop_duplicates(subset = ['href'])
    return(data)
    
def match_ecalendar(mf_source):
    fx678_source = aggregate_data(daterange)
    fx678_source = fx678_source[['href', 'actual_price']]
    mixed_tbl = pd.merge(mf_source,
                         fx678_source,
                         how = 'left',
                         on = 'href').drop_duplicates(subset = ['ID']).reset_index(drop = True)
    return(mixed_tbl)

ecal_tbl = match_ecalendar(ecal_tbl)
cursor = db.cursor()
update_tbl = ecal_tbl[ecal_tbl.actual_price.notnull()].reset_index(drop = True)
for i in update_tbl.index:
    upt_time = datetime.datetime.now().replace(microsecond=0)
    actual = float(update_tbl.loc[i,'actual_price'])
    en_multiplier = int(update_tbl.loc[i,'en_multiplier'])
    cn_multiplier = int(update_tbl.loc[i,'cn_multiplier'])
    ID = str(update_tbl.loc[i,'ID'])
    sql_update1 = "UPDATE ecalendar_tbl SET actual=%s, upt_time = '%s' WHERE ID='%s' and lang='EN'" % (float(actual*en_multiplier), upt_time, ID)
    sql_update2 = "UPDATE ecalendar_tbl SET actual=%s, upt_time = '%s' WHERE ID='%s' and lang!='EN'" % (float(actual*cn_multiplier), upt_time, ID)
    cursor.execute(sql_update1)
    cursor.execute(sql_update2)
    
cursor.close() 
db.commit()
db.close()

print("OK")
