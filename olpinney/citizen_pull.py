'''
API requests from Citizen (https://citizen.com/explore)

Author:Olivia Pinney 
proj-ez
'''
import requests
import bs4
import csv
import json
import sqlite3
import os
import pandas as pd

COLS=['created_at', 'updated_at', 'title', 'address', 'city_code', 'location', 'neighborhood', 'categories', 'severity', '_geoloc', 'updates', 'objectID']


#personal notes
'''
%load_ext autoreload
%autoreload 2
import citizen_pull as c
test=c.get_request_chicago(limit=5)

dict_keys(['created_at', 'updated_at', 'title', 'address', 'city_code', 'location', 'neighborhood', 'categories', 
'ranking.level', 'ranking.has_video', 'ranking.views', 'ranking.notifications', 
'severity', '_geoloc', 'updates', 'objectID', '_highlightResult'])

#put into sql - only exclude rankings
#see if there are dupe object_ids
#pull all categories
#figure out time frame of guns
#pull all red severity

#create connection
#try and accept to make sure table exists

    cur = conn.cursor()
    insert_query = ''INSERT INTO public.profile_data VALUES %s''
    print("got to tuples")
    tpls = [tuple(x) for x in df.to_numpy()]
    print(tpls[1])

    psycopg2.extras.execute_values(cur, insert_query, tpls, template=temp)

you know how many columns there will be, so can just insert them
create the tuple, and just insert it 

make a first query file to see if the query exists
if it doesnt, then create query (create table with these columns)
have this create query in the code
else append 

'''

# url_ext=["theft","shooting","assualt","knife"]

PATH="olpinney/all.csv"

def citizen_refresh(path,search=None):
    #CSV_FILE_PATH=os.path.join(os.path.expanduser('~'),'olpinney', 'all.csv')
    #os.getcwd()+CSV_FILE_PATH
    results_json=get_incidents_chicago(limit=5)
    #was limit sufficient 

    results_df = pd.DataFrame(all)
    results_df.to_csv(path)
    return results_df

def create_citizen_sql(c,cols,name):
    try: 
        col_count=len(cols)
        #note that list type is not accepted 
        create_query="CREATE TABLE "+name+" ("+", ".join(cols)+")"
        c.execute(create_query)
    except sqlite3.OperationalError:
        print("table already exists")
    return 

    #.fetchall()
    return 

def type_convert(arg): #because list type is not suported 
    if isinstance(arg,list):
        return str(arg)
    else:
        return arg

def insert_citizen_sql(incidents,name="citizen"):
    connection = sqlite3.connect("all.sqlite3")
    c = connection.cursor()

    cols=COLS
    col_count=len(cols)

    x=[type(incidents[0][col]) for col in cols] #note that lists are creating an issue 
    print(x)
    create_citizen_sql(c,cols,name)

    insert_query ="INSERT INTO "+name+" ("+", ".join(cols)+") VALUES ("+", ".join(["?"]*col_count)+")"

    for i in incidents:
        tup=(tuple([type_convert(i[col]) for col in cols]))
        c.execute(insert_query, tup)


def get_saved_data(path):
    connection = sqlite3.connect("all.sqlite3")
    c = connection.cursor()

    #.fetchall()
    return

def get_incidents_chicago(limit=1,search=None):
    lat_long=(41.763221610079455,-87.75672119312583,41.93975658843911,-87.56055880687256)
    return get_incidents(lat_long,limit,search)

def get_incidents(lat_long,limit,search=None):
    api_url="https://citizen.com/api/incident/search?insideBoundingBox[0]={}&insideBoundingBox[1]={}&insideBoundingBox[2]={}&insideBoundingBox[3]={}&limit={}"
    params=[entry for entry in lat_long]
    params.append(limit)
    if search:
        api_url+="&q={}&"
        params.append(search)
        
    url=api_url.format(*params)
    request=requests.get(url) #add max-age cache-control for refreshes
    citizen_dict=request.json()
    return citizen_dict['hits']

if __name__ == "__main__":
    citizen_refresh()
