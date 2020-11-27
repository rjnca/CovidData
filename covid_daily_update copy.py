import json
import datetime
import pymysql.cursors
import requests
import os


def get_data_from_api():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    connection = pymysql.connect(
        host=os.environ.get("DB_IP"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASS"),
        charset="utf8mb4",
        db=os.environ.get("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
    )

    mycursor = connection.cursor()
    dateranges = [("2020-09-01", "2020-09-30")]
    # dateranges = [
    #     ("2020-03-01", "2020-03-31"),
    #     ("2020-04-01", "2020-04-30"),
    #     ("2020-05-01", "2020-05-31"),
    #     ("2020-06-01", "2020-06-30"),
    #     ("2020-07-01", "2020-07-31"),
    #     ("2020-08-01", "2020-08-31"),
    #     ("2020-09-01", "2020-09-30"),
    # ]
    for ranges in dateranges:
        start, stop = ranges
        print(f"Getting Data from API {start} to {stop}")
        url = f"https://api.covid19api.com/country/united-states?from={start}&to={stop}"
        try:
            jdata = requests.get(url)
        except Exception as e:
            print(e)

        content = jdata.json()

        print("Cleaning Data")
        # Convert Date field to Date Type
        for items in content:
            newdate = datetime.datetime.strptime(
                items["Date"], "%Y-%m-%dT%H:%M:%SZ"
            ).date()
            items["NewDate"] = newdate
        newdict = []
        newdict = [i for i in content]

        totalfix = {"Province": "United States", "City": "Total", "CityCode": "00001"}
        cityfix = {"City": "All Locations"}
        kccitycodefix = {"CityCode": "99990"}
        for entry in newdict:
            if entry["Province"] == "" and entry["City"] == "":
                entry.update(totalfix)
            if entry["Province"] != "" and entry["City"] == "":
                entry.update(cityfix)
            if entry["Province"] == "Missouri" and entry["City"] == "Kansas City":
                entry.update(kccitycodefix)

        reccount = 0
        print("Updating City Table")
        citycount = 0
        for statedata in content:
            data = list(statedata.values())
            citydata = (data[0], data[1], data[2], data[3], data[4], data[5], data[6])
            try:
                city_sql = """INSERT INTO cityinfo (country,countrycode,province,city,citycode,lat,lon) VALUES(%s, %s, %s,
                %s, %s, %s, %s) ON DUPLICATE KEY UPDATE citycode=citycode;"""
                mycursor.execute(city_sql, citydata)
                reccount += 1
                if reccount > 10:
                    connection.commit()
            except Exception as e:
                print(e)
            connection.commit()
            citycount += 1
        print(f"{reccount} city rows inserted")
        print("Updating Data Table")
        covrowcount = 0
        for statedata in content:
            newdate = str(
                datetime.datetime.strptime(
                    statedata["Date"], "%Y-%m-%dT%H:%M:%SZ"
                ).date()
            )
            data = list(statedata.values())
            coviddata = (data[4], data[7], data[8], data[9], data[10], newdate)
            try:
                covid_sql = """INSERT INTO coviddata (citycode,confirmed,deaths,recovered,active_cases,report_date)
                VALUES(%s, %s, %s, %s, %s, %s) ;"""
                mycursor.execute(covid_sql, coviddata)
                covrowcount += 1
                if covrowcount > 10:
                    connection.commit()
            except Exception as e:
                print(e)
        connection.commit()

        print(f"{covrowcount} data records inserted")
        print("Update Complete")
    connection.close()


start = datetime.datetime.now()
get_data_from_api()
end = datetime.datetime.now()
print(f"Elapsed time: {end - start}")
