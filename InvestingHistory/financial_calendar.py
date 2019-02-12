# =============================================================================
#                           Financial_calednar V.2.py
# Version history:
# V.2: remove level as a matching key;
#      add a new column to specify those events can be updated by this program
#      cater all Final/Prelim/s.a./n.s.a data type
#
# V.1: original version by Elvis
# =============================================================================
import requests
import os
import mysql.connector as sql
import pandas as pd
import numpy as np
import datetime
from lxml import html
from datetime import timedelta
import pytz
from dateutil import tz
import configparser

#Set working directory
path = 'C:\\Python27\\Scripts\\AutoeCalendar'
os.chdir(path)

#Connect with Market Watch CMS environment
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

#Read matching table for events and 4 digits number related to investing.com
#csv_file = pd.read_csv("investing_tbl.csv")
#xls_file = pd.ExcelFile('/mnt/c/Elvis/eCalendar/ecalendar_info_20171221.xlsx')
#investing_tbl = xls_file.parse('investing_tbl', skiprows = 1)
#investing_tbl = csv_file[csv_file.country.notnull() & csv_file.level.notnull()]
#investing_tbl.loc[:,('data_type')] = investing_tbl.loc[:,('data_type')].fillna('')
#investing_tbl = investing_tbl.loc[:,['event', 'investing_link', 'country', 'level','indicator_name','data_type', 'en_multiplier','cn_multiplier']]
investing_tbl = pd.read_sql("SELECT event, investing_link, country, indicator_name, data_type, en_multiplier, cn_multiplier,id FROM investing_tbl", con = db)

#Import replace character information
character = pd.read_csv("replace_character.csv")

#Check event announce_time and hour
date_time = datetime.datetime.now().replace(microsecond=0)
#date_time = datetime.datetime(2018,11,29, 0,0,0)
date = date_time.date()
time = date_time.time()
t = timedelta(hours = time.hour,minutes = time.minute, seconds = time.second)

#ecal_tbl = pd.read_sql("SELECT ID, lang, level, country, actual, forecast, previous, announce_time, announce_hour, indicator_name, data_type, revise_previous from ecalendar_tbl where actual is NULL",
#                       con = db)

#Get dictionary table and ecalendar tables
ecal_tbl = pd.read_sql("SELECT ID, lang, level, country, actual, forecast, previous, announce_time, announce_hour, indicator_name, data_type, revise_previous, update_status from ecalendar_tbl where actual is NULL and announce_time = '%s'" % (date),
                       con = db)
#ecal_tbl = pd.read_sql("SELECT ID, lang, level, country, actual, forecast, previous, announce_time, announce_hour, indicator_name, data_type, revise_previous, update_status from ecalendar_tbl where announce_time = '%s'" % (date),
#                       con = db)


dict_tbl = pd.read_sql('SELECT id, type, code, en_text, disp_order, delete_flg from dictionary_tbl',
                 con = db)
#Testing
cols = ['actual', 'forecast', 'previous']
#ecal_tbl[cols] = np.nan

# =============================================================================
# Cater Final/Prelim data type issue
# =============================================================================
temp1 = pd.merge(investing_tbl, 
                 dict_tbl[['type', 'code', 'en_text']], 
                 how = 'left',
                 left_on = 'data_type',
                 right_on = 'code')

cater_data_type_tbl = temp1[temp1.en_text.str.contains("Prelim", na=False)|temp1.en_text.str.contains("Final", na=False)]
cater_data_type_tbl.loc[:,'cater_type'] = None
cater_data_type_tbl.loc[cater_data_type_tbl.en_text.str.contains("Final"), 'cater_type'] = cater_data_type_tbl.loc[cater_data_type_tbl.en_text.str.contains("Final"), 'en_text'].replace('Final', 'Preliminary', regex = True)
cater_data_type_tbl.loc[cater_data_type_tbl.en_text.str.contains("Preliminary"), 'cater_type'] = cater_data_type_tbl.loc[cater_data_type_tbl.en_text.str.contains("Preliminary"), 'en_text'].replace('Preliminary', 'Final', regex = True)

temp_cater_merge_tbl = pd.merge(cater_data_type_tbl, 
                            dict_tbl[['code', 'en_text']],
                            how = 'left',
                            left_on = 'cater_type',
                            right_on = 'en_text')
temp_cater_merge_tbl = temp_cater_merge_tbl[['investing_link','code_y']].rename(columns = {'code_y':'cater_type'})

investing_tbl_append = pd.merge(investing_tbl,
                         temp_cater_merge_tbl,
                         how = 'left',
                         on = 'investing_link')

temp2 = investing_tbl_append[investing_tbl_append.cater_type.notnull()]
temp2.loc[:,'data_type'] = temp2.loc[:,'cater_type']
temp2 = temp2.iloc[:,:-1]
investing_tbl = investing_tbl.append(temp2)
#investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'],
#                                              keep = False)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])

temp2 = temp1[(temp1.en_text== "(Final)") |(temp1.en_text== "(Preliminary)")]
temp2.loc[:,'data_type'] = ''
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
#investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'],
#                                              keep = False)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])

temp2 =temp1[temp1.en_text.str.contains("Prelim", na=False)|temp1.en_text.str.contains("Final", na=False)]
temp2 = temp2[(temp2.en_text!= "(Final)")]
temp2 = temp2[(temp2.en_text!= "(Preliminary)")]
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'].replace('Final', '', regex = True).str.strip()
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'].replace('Preliminary', '', regex = True).str.strip()
temp2 = pd.merge(temp2, dict_tbl[['code', 'en_text']],
                 how = 'left',
                 on = 'en_text')
temp2.loc[:,'data_type'] = temp2.loc[:, 'code_y']
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
#investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'],
#                                              keep = False)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])

temp2 = temp1[(temp1.data_type=="")|(temp1.en_text=="(MoM)")|(temp1.en_text=="(YoY)")|(temp1.en_text=="(QoQ)")|(temp1.en_text=="(MoM)")]
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'] .fillna('')
temp2.loc[temp2.en_text!="",'en_text'] = "Final " + temp2.loc[:,'en_text'].str.strip()
temp2.loc[temp2.en_text=="",'en_text'] = "(Final)" + temp2.loc[:,'en_text'].str.strip()
temp2 = pd.merge(temp2, dict_tbl[['code', 'en_text']],
                 how = 'left',
                 on = 'en_text')
temp2.loc[:,'data_type'] = temp2.loc[:, 'code_y']
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])

temp2 = temp1[(temp1.data_type=="")|(temp1.en_text=="(MoM)")|(temp1.en_text=="(YoY)")|(temp1.en_text=="(QoQ)")|(temp1.en_text=="(MoM)")]
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'].fillna('')
temp2.loc[temp2.en_text!="",'en_text'] = "Preliminary " + temp2.loc[:,'en_text'].str.strip()
temp2.loc[temp2.en_text=="",'en_text'] = "(Preliminary)" + temp2.loc[:,'en_text'].str.strip()
temp2 = pd.merge(temp2, dict_tbl[['code', 'en_text']],
                 how = 'left',
                 on = 'en_text')
temp2.loc[:,'data_type'] = temp2.loc[:, 'code_y']
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
investing_tbl = investing_tbl.replace(np.nan, '', regex = True)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])
# =============================================================================
# Final cater Final/Prelim
# =============================================================================
# =============================================================================
# Cater n.s.a/s.a data type issue
# =============================================================================
temp2 =temp1[temp1.en_text.str.contains("s.a.", na=False)]
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'].replace('s.a.', '', regex = True).str.strip()
temp2 = pd.merge(temp2, dict_tbl[['code', 'en_text']],
                 how = 'left',
                 on = 'en_text')
temp2 = temp2[(temp2.en_text!="")]
temp2.loc[:,'data_type'] = temp2.loc[:, 'code_y']
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
#investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'],
#                                              keep = False)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])


temp2 =temp1[temp1.en_text.str.contains("s.a.", na=False)]
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'].replace('s.a.', '', regex = True).str.strip()
temp2 = pd.merge(temp2, dict_tbl[['code', 'en_text']],
                 how = 'left',
                 on = 'en_text')
temp2 = temp2[(temp2.en_text=="")]
temp2.loc[:,'data_type'] = ''
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
#investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'],
#                                              keep = False)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])

temp2 = temp1[(temp1.data_type=="")|(temp1.en_text=="(MoM)")|(temp1.en_text=="(YoY)")|(temp1.en_text=="(QoQ)")|(temp1.en_text=="(MoM)")]
temp2.loc[:,'en_text'] = temp2.loc[:,'en_text'] .fillna('')
temp2.loc[temp2.en_text!="",'en_text'] = "s.a. " + temp2.loc[:,'en_text'].str.strip()
temp2.loc[temp2.en_text=="",'en_text'] = "s.a." + temp2.loc[:,'en_text'].str.strip()
temp2 = pd.merge(temp2, dict_tbl[['code', 'en_text']],
                 how = 'left',
                 on = 'en_text')
temp2.loc[:,'data_type'] = temp2.loc[:, 'code_y']
temp2 = temp2.iloc[:,0:8]
investing_tbl = investing_tbl.append(temp2)
investing_tbl = investing_tbl.drop_duplicates(subset = ['event', 'investing_link', 'country', 'indicator_name', 'data_type'])
# =============================================================================
# Final cater n.s.a
# =============================================================================
##################################################################
#temp = pd.merge(ecal_tbl, 
#               investing_tbl,
#               how = 'left', 
#               on = ['country', 'level','indicator_name','data_type'])

temp = pd.merge(ecal_tbl, 
               investing_tbl,
               how = 'left', 
               on = ['country','indicator_name','data_type'])

#bool = (temp['announce_time'] == date) & (temp['announce_hour'] > t)
bool = (temp['announce_time'] == date)
#bool = temp['announce_time'] >= date
temp_raw = temp.loc[bool]
temp = temp.loc[bool & temp.investing_link.notnull()]
temp = temp.drop_duplicates()
temp = temp.drop_duplicates(subset = 'ID', keep = 'last')
# =============================================================================
# Determine the data can be updated by robot
cursor = db.cursor()
for i in temp.ID:
    sql_update = "UPDATE ecalendar_tbl SET update_status = 'A' WHERE ID='%s'" % (i)
    cursor.execute(sql_update)
        
cursor.close()
# =============================================================================
#Extract information from investing.com
def investing_scrapper(this_item):
    global investing_info
    url_base = "https://www.investing.com/economic-calendar/%s"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}

    try:
        page = requests.get(url_base % this_item, headers=headers)
    except Exception, e:
        print(e.message)
        return None

    tree = html.fromstring(page.content)
    from_zone = tz.gettz('UTC')
    local_tz = pytz.timezone ("Asia/Hong_Kong")
    
    name = tree.xpath("//div[@class='content']/p/span[@class='js-alert-name']/text()")[0]
    time_stamp = tree.xpath('//div[@class = "historyTab"]/table[@id="eventHistoryTable%s"]/tbody/tr/@event_timestamp' % this_item)
    time_stamp = [datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S") for time in time_stamp]
    
    for index, item in enumerate(time_stamp):
        time_stamp[index] = time_stamp[index].replace(tzinfo = from_zone)
        time_stamp[index] = time_stamp[index].astimezone(local_tz).date()
    
    try:
        indexes = [i for i,times in enumerate(time_stamp) if times == date][0] + 1
    except IndexError:
        return None
    
    try:
        release_date = tree.xpath('//*[@id="eventHistoryTable%s"]/tbody/tr[%s]/@event_timestamp' % (this_item,indexes))[0] 
    except IndexError:
        release_date = None
        
    utc = datetime.datetime.strptime(release_date, "%Y-%m-%d %H:%M:%S")
    utc = utc.replace(tzinfo = from_zone)
    release_date = utc.astimezone(local_tz).date()
    #release_date = datetime.datetime.strptime(release_date, '%b %d, %Y')
    
    try:
        actual = tree.xpath('//div[@class = "historyTab"]/table[@id="eventHistoryTable%s"]/tbody/tr[%s]/td[3]/span/text()' % (this_item,indexes))[0]
    except IndexError:
        try:
            actual = None
        except IndexError:
            pass
            
    try:
        previous = tree.xpath('//div[@class = "historyTab"]/table[@id="eventHistoryTable%s"]/tbody/tr[%s]/td[5]/text()' % (this_item,indexes))[0]
    except IndexError:
        try:
            previous = None
        except IndexError:
            pass
            
    try:
        forecast = tree.xpath('//div[@class = "historyTab"]/table[@id="eventHistoryTable%s"]/tbody/tr[%s]/td[4]/text()' % (this_item,indexes))[0]
    except IndexError:
        try:
            forecast = None
        except IndexError:
            pass
                    
    columns = ['name', 'release_date', 'actual', 'forecast', 'previous']
    cols = ['actual', 'forecast', 'previous']
    investing_info = pd.DataFrame(data = {'name':name, 
                                'release_date':release_date, 
                                'actual':actual, 
                                'forecast':forecast, 
                                'previous':previous},
                                 columns = columns,
                                 index=[0])

#    investing_info[cols] = investing_info[cols].replace({'%':'', 
#                                                         'T':'', 
#                                                         'B':'',
#                                                         'K':'',
#                                                         ',':'',
#                                                         'M':''}, regex=True)
    for i in range(len(character)):
        investing_info[cols] = investing_info[cols].replace(character.iloc[i,0],'',regex = True) 
    
    investing_info[cols] = investing_info[cols].apply(pd.to_numeric, errors = 'coerce')
    
    return investing_info

target = temp.investing_link
target = list(map(str,map(int,target)))

for i in target:
    
    investing_info = None
    del investing_info
    info = investing_scrapper(i)
    #mask = (temp['investing_link']==int(i)) & (temp['actual'].isnull())
    try:
        investing_info
    except NameError:
        var_exists = False
    else:
        var_exists = True
        
    if(var_exists):
        mask = (temp['investing_link']==int(i)) & (temp['announce_time']== investing_info['release_date'][0])
        temp.loc[mask,cols] = investing_info[cols].values

# =============================================================================
# #Update values to database
# =============================================================================

cursor = db.cursor()

target = temp.ID[temp['actual'] == temp['actual']]
for j in target:
    mask = (ecal_tbl['ID'] == j) & (ecal_tbl['lang']!= 'EN')
    mask_en = (ecal_tbl['ID'] == j) & (ecal_tbl['lang']== 'EN')
    mask_temp = temp['ID'] == j
    ecal_tbl.loc[mask_en,cols] = temp[mask_temp][cols].values*temp[mask_temp]['en_multiplier'].values #Change to the original database location
    ecal_tbl.loc[mask,cols] = temp[mask_temp][cols].values*temp[mask_temp]['cn_multiplier'].values
    en_multiplier = temp[mask_temp]['en_multiplier'].values[0]
    cn_multiplier = temp[mask_temp]['cn_multiplier'].values[0]
    actual =  temp.loc[temp['ID'] == j]['actual'].values[0]
    upt_time = datetime.datetime.now().replace(microsecond=0)
	
    if actual!= actual:
        actual = None
    else:
        actual = float(actual)
    forecast = temp.loc[temp['ID'] == j]['forecast'].values[0]
    if forecast!= forecast:
        forecast = None
    else:
        forecast = float(forecast)
    previous = temp.loc[temp['ID'] == j]['previous'].values[0]
    if previous!= previous:
        previous = None
    else:
        previous = float(previous)
    if forecast == None:
        sql_update1 = "UPDATE ecalendar_tbl SET actual=%s, forecast=NULL, previous=%s, upt_time = '%s' WHERE ID='%s' and lang='EN'" % (float(actual*en_multiplier), float(previous*en_multiplier), upt_time, j)
        sql_update2 = "UPDATE ecalendar_tbl SET actual=%s, forecast=NULL, previous=%s, upt_time = '%s' WHERE ID='%s' and lang!='EN'" % (float(actual*cn_multiplier), float(previous*cn_multiplier), upt_time, j)
    else:
        sql_update1 = "UPDATE ecalendar_tbl SET actual=%s, forecast=%s, previous=%s, upt_time = '%s' WHERE ID='%s' and lang='EN'" % (float(actual*en_multiplier), float(forecast*en_multiplier), float(previous*en_multiplier), upt_time, j)
        sql_update2 = "UPDATE ecalendar_tbl SET actual=%s, forecast=%s, previous=%s, upt_time = '%s' WHERE ID='%s' and lang!='EN'" % (float(actual*cn_multiplier), float(forecast*cn_multiplier), float(previous*cn_multiplier), upt_time, j)
    cursor.execute(sql_update1)
    cursor.execute(sql_update2)
 
cursor.close()
db.commit()
db.close()
