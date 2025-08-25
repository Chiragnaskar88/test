import requests
import pandas as pd
from sqlalchemy import create_engine


def fetchData(Startyear,Endyear):
    frecord = []
    for i in range(Startyear,Endyear):
        syear = i
        eyear = i+1
        res = requests.get(f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={syear}-01-01&endtime={eyear}-01-01&minmagnitude=4")
        data = res.json()
        e_lst = data['features']
        for quake in e_lst:
            prop = quake['properties']
            geo = quake['geometry']
            cord = geo['coordinates']
            frecord.append({
                "mag" : prop["mag"],
                "time" : prop['time'],
                'place':prop['place'],
                'tsunami':prop['tsunami'],
                'sig':prop['sig'],
                'net':prop['net'],
                'nst': prop['nst'],
                'magType': prop['magType'],
                'rms': prop['rms'],
                'gap': prop['gap'],
                'dmin':prop['dmin'],
                'type': prop['type'],
                'latitude': cord[1],
                'longitude': cord[0],
                'dept': cord[2],
                "updated": prop["updated"],
                "tz":prop["tz"],
                "felt": prop["felt"],
                "cdi":prop["cdi"],
                "mmi": prop["mmi"],
                "alert":prop["alert"],
                "status":prop["status"],
                "code": prop["code"],
                "ids": prop["ids"],
                "types": prop["types"],
                "title": prop["title"]
                
                
            })
    return frecord

def rawtopostgres(data,rawtable):
    df = pd.DataFrame(data)
    df['time'] = pd.to_datetime(df['time'],unit='ms')
    engine = create_engine('postgresql+psycopg2://myuser:mypassword@localhost:5432/earthquake', echo=False)
    df.to_sql(name=rawtable,con=engine,index=False, if_exists='append')
    
data  = fetchData(2024,2025)
table_name = "concourse_RawTable"
print("data fetching done")
print("darta storing to postgres")
rawtopostgres(data,table_name)
print("Data stored in postgres")