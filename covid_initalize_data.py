import pymysql.cursors
import requests
import json
import datetime
import os


def content_download():
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    url = f"https://api.covid19api.com/country/united-states?from=2020-03-01T00:00:00Z&to={today}"

    print("Getting Data from API")
    try:
        jdata = requests.get(url)
    except Exception as e:
        print(e)

    content = jdata.json()

    newdict = clean_data(content)

    global dlreccount
    print("Starting Write")
    with open("results.json", "w") as result:
        json.dump(newdict, result, indent=2)
        for city in newdict:
            dlreccount += 1

    print(f"{dlreccount} records retrieved")
    return dlreccount


def clean_data(dlcontent):
    print("Cleaning Content")

    totalfix = {"Province": "United States", "City": "Total", "CityCode": "00001"}
    cityfix = {"City": "All Locations"}
    kccitycodefix = {"CityCode": "99990"}

    for entry in dlcontent:
        if entry["Province"] == "" and entry["City"] == "":
            entry.update(totalfix)
        if entry["Province"] != "" and entry["City"] == "":
            entry.update(cityfix)
        if entry["Province"] == "Missouri" and entry["City"] == "Kansas City":
            entry.update(kccitycodefix)

    return dlcontent


def update_db():
    connection = pymysql.connect(
        host=os.environ.get("DB_IP"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASS"),
        charset="utf8mb4",
        db=os.environ.get("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
    )

    mycursor = connection.cursor()

    with open("results.json") as f:
        content = json.load(f)

    reccount = 0
    print("Updating City Table")
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
    print(f"{reccount} city rows inserted")
    print("Updating Data Table")
    covrowcount = 0
    for statedata in content:
        newdate = str(
            datetime.datetime.strptime(statedata["Date"], "%Y-%m-%dT%H:%M:%SZ").date()
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
    connection.close()
    print(f"{covrowcount} data records inserted")
    print("Update Complete")


start = datetime.datetime.now()

dlreccount = 0

dlreccount = content_download()
if dlreccount > 0:
    update_db()
else:
    print("No records to update")

end = datetime.datetime.now()
print(f"Elapsed Time: {end - start}")
