
'''
commands for running citizen_pull.py

%load_ext autoreload
%autoreload 2

import citizen_pull as c

# to refresh run this:
c.citizen_refresh() 
OR
c.citizen_searches_refresh()
#Notes: limit = int is optional. ~500 incidences happen per day, but max of 1000 can be pulled at once

#to delete sql object and create a new one:
c.reset_citizen(limit) #pick a 1000 limit 
c.citizen_backfill() #to backfill with old data 
OR
c.citizen_searches_reset_and_backfill()

#To see the results
x=c.citizen_get_sql(search="{desired search term}",table_name="citizen")

x=c.citizen_get_sql(table_name="citizen")


#to pull from citizen 
y=c.get_incidents_chicago(limit) #select limit= how many rows you want
y=c.clean_citizen(y)

#to open sqlite3
sqlite3 proj_ez.sqlite3

import os
import sys
os.path.dirname(sys.executable)
'''