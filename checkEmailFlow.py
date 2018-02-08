#!/usr/bin/python3.5

import mysql.connector
from mysql.connector import errorcode
import logging
import datetime
import csv

Logger = logging.getLogger(__name__)
# create a file handler
handler = logging.FileHandler('mysqlscript.log')
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(filename)s - %(message)s')
handler.setFormatter(formatter)
# add the handlers to the logger
Logger.addHandler(handler)


#config file used to connect with database
config = {
  'user': 'root',
  'password': '123456',
  'host': '127.0.0.1',
  'database': 'testscript'
 }

data = {
    'rt01' : '20643',
    'rt02' : ['20645','20644']
}

metaData = {
    'date' : '',
    'tableName' : ''
}


#function to make a connection with data base
def connDB(configFile):
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            pass
            Logger.warning("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            Logger.warning("Database does not exist")
    else:
        Logger.info("Sucesfully connected with Msql")
        return cnx

#function to return mysql cursor object
def getCursor(conn):
    return conn.cursor()

#function ro run query
def runQuery(cur,query):
    try :
        cur.execute(query)
    except mysql.connector.Error as err:
        infoLogger.info(err)
        exit(1)
    else:
        return cur

def returnTime(time):
    return datetime.datetime.fromtimestamp(time)

def dataInOrder(data1, data2):
     filterData = (  (x[0] , returnTime(y[1]), returnTime(x[1]), (returnTime(y[1]) -  returnTime(x[1])).seconds)  for x in data1 for y in data2 if x[0] == y[0] )
     return filterData

def writeToCsvFile(filename, orderedData):
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter='\t')
        format1 = " Subscriber Id   ,", "   First Mail Opened   ,", "   First RT1 Opened    ,","    Time Difference "
        writer.writerow(format1)
        #writer = csv.DictWriter(csvfile, fieldnames = ["Subscriber Id", "First Mail Opened", "First RT1 Opened", "Time Difference"])
        for subid, firsttime, secondtime, diff in orderedData:
            row = subid, "," ,firsttime, "," ,secondtime, ",",diff
            try:
                writer.writerow(row)
            except TypeError:
                print(TypeError)

def closeConn(conn):
    conn.close()

#running main python code here
if __name__ == "__main__":

    db = connDB(config)

    query1 = ("""select subscriberid,opentime from
                (select * from ss_stats_emailopens where from_unixtime(opentime) like "%2018-02-01%" and statid NOT IN (20643,20645,20644) order by opentime)
                 as firstquery group by subscriberid; """)

    query2 = ("""select subscriberid, opentime
               from (select * from ss_stats_emailopens where from_unixtime(opentime) like "%2018-02-01%" and statid IN (20643) order by opentime)
               as firstquery group by subscriberid""")

    cursor1 = getCursor(db)
    runQuery(cursor1, query1)
    data1 = cursor1.fetchall()
    cursor1.close()

    cursor2 = getCursor(db)
    runQuery(cursor2, query2)
    data2 = cursor2.fetchall()
    cursor2.close()

    orderedData = dataInOrder(data1,data2)

    writeToCsvFile("timediff.csv", orderedData)

    closeConn(db)
