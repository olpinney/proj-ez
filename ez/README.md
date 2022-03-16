Please include a README.md that explains how to quickly install [if necessary] and your project. Include a few examples about how to interact with your project within the README document. This document is separate from your paper that provides more details about the project. The README is used to quickly start running your project.

To run our project:
1. cd to directry ez (from proj-ez inside environmnet after installing requirements)
2. in command line run python3 __main__.py. This will call our record_link.py file and our server.py
which are the two main components of our analysis. The record link compares data between citizen and
chicago crime data portal, and the server creates visual of the 'mock' incidents that ECCSC responded to. 

Additionally to test our Chicago Data Portal API and Citizen web scraper you can navigate to the refresh_data 
module and run the python3 police_api.py, as well as python3 citizen_pull.py. This data is store in our 
proj_ez sqlite3 database, where our record_link.py and server.py files pull their data from. 

Notes:
If the map server is in use updating the last line in the servery.py file located in the map directory to 
app.run_server(port=1234) will fix this.

Additinally if geoloc timesout, updating locator variable in get_lat_long function in sql_query in refresh_data module  
will fix this.
