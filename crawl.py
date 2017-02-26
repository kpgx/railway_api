from itertools import permutations
import sqlite3
import json
from pprint import pprint
import requests
#import urllib.request

station_list=['218','9','171','287','75']
search_date='2017-2-28'
start_time='00:00:00'
end_time='23:59:59'
lang='en'
day='sun'

host='http://railway.lankagate.gov.lk'
path='/train/searchTrain'
station_path='/station/getAll'

def main():
    #createTables()
    #updateStationList()
    updateTheCache()

def updateTheCache(start =0):
    st_list=getStationListFromDB()
    st_perm=list(permutations(st_list,2))
    st_perm.sort()
    for i in range(len(st_perm)-start):
        perm=st_perm[i+start]
        print (perm)
        start_station_id=perm[0][0]
        end_station_id=perm[1][0]
        url='%s%s?startStationID=%s&endStationID=%s&searchDate=%s&startTime=%s&endTime=%s&lang=%s'%(host,path,start_station_id,end_station_id,search_date,start_time,end_time,lang)
        server_reply=getReplyForUrl(url)
        if server_reply:
            processJourney(server_reply,start_station_id)

def processJourney(txt,st_id):
    json_data=json.loads(txt)
    d_trains=json_data['RESULTS']['directTrains']['trainsList']
    c_trains=json_data['RESULTS']['connectingTrains']['trainsList']
    for train in d_trains:
        processDTrain(train,st_id)
    for train in c_trains:
        processCTrain(train,st_id)

def processDTrain(train,st_id):
    j_id=train['trainID']
    num=train['trainNo']
    fin_st_id=getStID(train['finalStationName'].title())
    fin_ar_time=train['arrivalTimeFinalStation']
    arr_time=train['arrivalTime']
    dep_time=train['depatureTime']
    t_type=' '.join(train['trainType'].split()).title()
    t_name=' '.join(train['trainName'].split()).title()
    class_list=getClassList(train['classList'])
    addTrain(j_id,num,st_id,fin_st_id,fin_ar_time,arr_time,dep_time,t_type,t_name,class_list)

def getStID(name):
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute('SELECT id FROM station WHERE name=?',(name,))
    r_val=c.fetchone()
    conn.close()
    return r_val[0]

def addTrain(j_id,t_id,st_id,fin_st_id,fin_time,a_time,d_time,j_type,t_name,c_list):
    #print (j_id,t_id,fin_st_id,fin_time,a_time,d_time,j_type,t_name,c_list)
    type_id=addToType(j_type)
    print (type_id)
    addToJourney(j_id,t_id,type_id,c_list)
    addFrequency(j_id)
    addToStop(j_id,st_id,a_time,d_time)
    addToStop(j_id,fin_st_id,fin_time,'')
    addToTrain(t_id,t_name)

def addToType(name):
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute('SELECT id FROM type WHERE name=?',(name,))
    r_val=c.fetchone()
    if not r_val:
        c.execute('INSERT INTO type (name) VALUES (?)',(name,)) 
        conn.commit()
        c.execute('SELECT id FROM type WHERE name=?',(name,))
        r_val=c.fetchone()
    conn.close()
    return r_val[0]

def addToJourney(j_id,t_id,type_id,class_list):
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute('SELECT id FROM journey WHERE id=?',(j_id,))
    r_val=c.fetchone()
    if not r_val:
        c.execute('INSERT INTO journey (id,train_id,type_id,class_list) VALUES (?,?,?,?)',(j_id,t_id,type_id,class_list)) 
        conn.commit()
    conn.close()

def addFrequency(j_id):
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    sql='UPDATE journey SET %s=1 WHERE id=?'%day
    c.execute(sql,(j_id,))
    conn.commit()
    conn.close()

def addToStop(j_id,st_id,arr_t,dep_t):
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute('SELECT id FROM stop WHERE id=? AND station_id=?',(j_id,st_id))
    r_val=c.fetchone()
    if not r_val:
        c.execute('INSERT INTO stop (id,station_id,reach_time,departure_time) VALUES (?,?,?,?)',(j_id,st_id,arr_t,dep_t)) 
        conn.commit()
    conn.close()

def addToTrain(t_id,t_name):
    print (t_id,t_name)
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute('SELECT id FROM train WHERE id=?',(t_id,))
    r_val=c.fetchone()
    if not r_val:
        c.execute('INSERT INTO train (id,name) VALUES (?,?)',(t_id,t_name)) 
        conn.commit()
    conn.close()

def getClassList(cl):
    l=[str(i['classID']) for i in cl]
    l.sort()
    return ','.join(l)

def processCTrain(train,st_id):
    print (train)

def getStationListFromDB():
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute("SELECT id FROM station")
    r_val=c.fetchall()
    conn.close()
    return r_val

def getReplyForUrl(url):
    print (url)
    r=requests.get(url)
    code=r.status_code
    if code!=200:
        print(code)
        return None
    return r.content

def updateStationList():
    url='%s%s'%(host,station_path)
    txt=getReplyForUrl(url)
    if txt:
        conn=sqlite3.connect('railway.db')
        c=conn.cursor()
        json_data=json.loads(txt)
        for station in json_data['RESULTS']['stationList']:
            print(station['stationID'],station['stationName'].title())
            c.execute("INSERT INTO station VALUES(?,?)",(station['stationID'],station['stationName'].title()))
            conn.commit()
        conn.close()
    else:
        print('Server error')

def createTables():
    conn=sqlite3.connect('railway.db')
    c=conn.cursor()
    c.execute('''CREATE TABLE station (id INTEGER, name TEXT)''')
    c.execute('''CREATE TABLE journey (id INTEGER, name TEXT, train_id INTEGER, type_id INTEGER)''')
    c.execute('''CREATE TABLE stop (id INTEGER, station_id INTEGER, reach_time TEXT, departure_time TEXT)''')
    c.execute('''CREATE TABLE train (id INTEGER, name TEXT)''')
    c.execute('''CREATE TABLE type (id INTEGER, name TEXT)''')
    conn.commit()
    conn.close()
    
main()
