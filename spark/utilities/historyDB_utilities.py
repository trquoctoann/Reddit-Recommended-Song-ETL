import mysql.connector
import datetime

def check_database_existence(mycursor) :
    mycursor.execute('show databases')
    exist = False
    for database in mycursor : 
        if database[0] == 'historyDB' : 
            exist = True 
            break
    myresult = mycursor.fetchall()
    return exist

def check_table_existence(mycursor) : 
    mycursor.execute('show tables')
    exist = False
    for table in mycursor : 
        if table[0] == 'executionHistory' : 
            exist = True 
            break
    myresult = mycursor.fetchall()
    return exist

def mysql_connector() : 
    connector = mysql.connector.connect(host = 'history-database', user = 'root', password = '123', port = int(3306))
    mycursor = connector.cursor()
    database_existence = check_database_existence(mycursor)
    if database_existence is False : 
        mycursor.execute('CREATE DATABASE historyDB')
    mycursor.execute('USE historyDB')
    table_existence = check_table_existence(mycursor)
    if table_existence is False : 
        mycursor.execute('CREATE TABLE executionHistory (rMusic_last_time DATETIME, rIndieHeads_last_time DATETIME, \
                         rPopHeads_last_time DATETIME, rElectronicMusic_last_time DATETIME)')
        dml_insertion = "INSERT INTO executionHistory (rMusic_last_time, rIndieHeads_last_time, rPopHeads_last_time, rElectronicMusic_last_time) \
                         VALUES (%s, %s, %s, %s)"
        value = [(datetime.date.today(), datetime.date.today(), datetime.date.today(), datetime.date.today())]
        mycursor.executemany(dml_insertion, value)
        connector.commit()
    return connector

def get_last_execution_date() : 
    connector = mysql_connector()
    mycursor = connector.cursor()
    mycursor.execute('USE historyDB')
    get_last_execution = "SELECT rMusic_last_time, rIndieHeads_last_time, rPopHeads_last_time, rElectronicMusic_last_time \
                          FROM executionHistory ORDER BY rMusic_last_time DESC LIMIT 1"
    mycursor.execute(get_last_execution)
    result = mycursor.fetchone()
    return result

def save_new_execution_date(rMusic_execution_time, rIndieHeads_execution_time, 
                            rPopHeads_execution_time, rElectronicMusic_execution_time) : 
    connector = mysql_connector()
    mycursor = connector.cursor()
    mycursor.execute('USE historyDB')
    dml_insertion = "INSERT INTO executionHistory (rMusic_last_time, rIndieHeads_last_time, rPopHeads_last_time, rElectronicMusic_last_time) \
                     VALUES (%s, %s, %s, %s)"
    value = [(rMusic_execution_time, rIndieHeads_execution_time, rPopHeads_execution_time, rElectronicMusic_execution_time)]
    mycursor.executemany(dml_insertion, value)
    connector.commit()
    print("Extraction datetime added to History-database")