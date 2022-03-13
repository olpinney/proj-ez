import pandas as pd
import sqlite3


latlong = [{'lat': 41.86378098645423, 'lng': -87.62695802730832},{'lat': 41.8203000313697, 'lng': -87.61341991257548},
{'lat': 41.87029003718416, 'lng': -87.73511994990292},{'lat': 41.84916005555968, 'lng': -87.6319299208149},
{'lat': 41.86991301917934, 'lng': -87.66197602621372},{'lat': 41.8784359897852, 'lng': -87.62648998183501},
{'lat': 41.91424800637842, 'lng': -87.70358697869982},{'lat': 41.89363302549994, 'lng': -87.72064727802713},
{'lat': 41.83843004602866, 'lng': -87.617459906087},{'lat': 41.89522003843226, 'lng': -87.73595990042014},
{'lat': 41.84206003874635, 'lng': -87.61754992772711},{'lat': 41.78004003212789, 'lng': -87.62534987536507},{'lat': 41.7841800217443, 'lng': -87.59313989784249},
{'lat': 41.92563004415746, 'lng': -87.63811995630996},{'lat': 41.80067003322796, 'lng': -87.71600988121517},{'lat': 41.87630099331787, 'lng': -87.6310609689127},
{'lat': 41.91859042614385, 'lng': -87.75425886297784},{'lat': 41.78009300575596, 'lng': -87.61381402966278},{'lat': 41.78143700202062, 'lng': -87.64026698080835},
{'lat': 41.88446002168789, 'lng': -87.63536993769713},{'lat': 41.88736003445806, 'lng': -87.64985990135459},{'lat': 41.88234199857448, 'lng': -87.63117697445264},
{'lat': 41.92425004761864, 'lng': -87.7462998999964},{'lat': 41.76843700547164, 'lng': -87.7234140353852},{'lat': 41.87945099635024, 'lng': -87.73434797662047},
{'lat': 41.92435004372354, 'lng': -87.73655987704944},{'lat': 41.88534401910627, 'lng': -87.69603102826329},{'lat': 41.79347006221928, 'lng': -87.70598990270216},
{'lat': 41.86597700317668, 'lng': -87.7156190330701},{'lat': 41.88325005205499, 'lng': -87.62943990883856},{'lat': 41.89544002148111, 'lng': -87.7211999556614},
{'lat': 41.78318999325074, 'lng': -87.58925597536809},{'lat': 41.810014731824, 'lng': -87.6038597168676},{'lat': 41.85194402356924, 'lng': -87.68556131711813},
{'lat': 41.8441850188394, 'lng': -87.71636301079596},{'lat': 41.88098002312751, 'lng': -87.70610993155563},{'lat': 41.85289398387675, 'lng': -87.68049802057469}]

connection = sqlite3.connect("proj_ez.sqlite3")
c = connection.cursor()

s = '''
       SELECT lat_long
       FROM citizen
       LIMIT 10
    '''

print('cursor', c)

#query = (c.execute(s).fetchall())

#df = pd.DataFrame(query)
#print(df)


##df = pd.DataFrame(query)

'''for coord in latlong:
    lat = coord['lat']
    long = coord['lng']
    print((lat,long))
    print(coord.values().to_list)'''


def latlong(coord):
    lat = coord['lat']
    long = coord['lng']
    return (lat,long)

####coordinates
df = pd.read_csv('neighborhood_cords.csv')



def str_lst(df, loc_col):
    all= []
    for i,row in df.iterrows():
        poly_str = row['the_geom']
        poly_str = poly_str[16:len(poly_str)-3]
        poly_lst = poly_str.split(",")    
        lat_longs = []
        for i,value in enumerate(poly_lst):
            lst = value.split(" ")
            if i == 0:
                lat = lst[0]
                long = lst[1] 
            else:
                lat = lst[1]
                long = lst[2]
            #lat_longs.append((float(lat), float(long)))
            lat_longs.append((lat, long))
        all.append(lat_longs)
    df['lat_longs'] = all

    return all

