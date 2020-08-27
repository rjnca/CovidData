import pymysql
import datetime
import os

connection = pymysql.connect(
    host=os.environ.get("DB_IP"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASS"),
    charset="utf8mb4",
    db=os.environ.get("DB_NAME"),
    cursorclass=pymysql.cursors.DictCursor,
)

mycursor = connection.cursor()


start = datetime.datetime.now()


createdb = "CREATE DATABASE covid"
drop_city_query = "drop table if exists cityinfo;"
drop_data_query = "drop table if exists coviddata;"
create_city_query = """CREATE TABLE cityinfo (country varchar(30), countrycode varchar(2), province varchar(25),
city varchar(25), citycode varchar(10) primary key unique, lat float, lon float)"""
create_data_query = """CREATE TABLE coviddata (id int primary key auto_increment, citycode varchar(10),
confirmed int, deaths int, recovered int, active_cases int, report_date date)"""


tb = "describe covid"


try:
    mycursor.execute(drop_city_query)
    connection.commit()
    print("City Table Deleted")
    mycursor.execute(drop_data_query)
    connection.commit()
    print("Data Table Deleted")
    mycursor.execute(create_city_query)
    connection.commit()
    print("CityInfo Table Created Successfully")
    mycursor.execute(create_data_query)
    connection.commit()
    print("CovidData Table Created Successfully")
except Exception as e:
    print("Exception error occured :", e)

connection.close()
end = datetime.datetime.now()
print(f"Elapsed Time: {end - start}")

