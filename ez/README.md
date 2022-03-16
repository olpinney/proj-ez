To run our project:

1. cd to directory ez (from proj-ez inside environmnet after installing requirements)
2. in command line run python3 __main__.py. This will call our record_link.py file and our server.py
which are the two main components of our analysis. The record link compares data between citizen and
chicago crime data portal, and the server creates visual of the 'mock' incidents that ECCSC responded to. 

To test our Chicago Data Portal API and Citizen web scraper you can navigate to the refresh_data 
module and run the python3 police_api.py, as well as python3 citizen_pull.py. This data is store in our 
proj_ez sqlite3 database, where our record_link.py and server.py files pull their data from. 

Key User Notes:

If the map server is in use updating the last line in the servery.py file located in the map directory to 
app.run_server(port=1234) will fix this.

Additionally if geoloc times out, update locator variable with new email address. Within code go to get_lat_long 
function in refresh_data/sql_query and insert new email address. 

These are the files contained in Ez package:

ez:
    - match_file.csv: putput of matches found between citizen and chicago data (output of record_link.py)
    - README.md: this file
    - requirements.txt: modules needed to import
    - app.py: 
    - projection_interaction.bat: file to interact with app from cammand line

    -refresh_data
        - citizen_pull.py: scrapes Citizen App for incident data and stores in sqlite database as well as backup csv files in data folder
        - proj_ez.sqlite3
        - refresh_citizen.bat: used in conjunction with task mananger to automate citizen pull to run daily  
        - util.py: helper functions for wokring with sql and csv
        - google_sheet.py: connects to ECCSC survey data and pulls to csv
        - police_api.py: Pulls Chicago crime data through an API and stores in sqlite database 
        - sql_query.py: connects to sql database to and creates pandas dataframes from data

    -link_records
        - record_link.py: finds matches between Chicago crime data and Citizen App data and identifies matches based on time and distance
        outputs to match_file.csv in ez 

    -map
        server.py: generates map of ECCSC incidents addressed

    -data
        -data 
            - contains historical data in csv files
