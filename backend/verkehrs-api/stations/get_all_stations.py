import pandas as pd
import os

def get_vienna_stations_from_url(url):
    
    # Daten laden (CSV)
    df = pd.read_csv(url, 
                 encoding='windows-1252',
                 on_bad_lines='skip')  # Neuere Pandas

    output_path = os.path.join(os.path.dirname(__file__), "vienna_stations_all_raw_rbl.csv")
    df.to_csv(output_path, index=False)

    return df

def get_vienna_stations_local(path):
    # Offizielle URL der Wiener Linien Haltestellen-Da    
    # Daten laden (CSV)
    df = pd.read_csv(path, 
                 encoding='windows-1252',
                 on_bad_lines='skip',
                 sep=';')  # Neuere Pandas

    return df

URL_STATIONS_RBL = "https://www.wienerlinien.at/ogd_realtime/doku/ogd/wienerlinien-ogd-haltepunkte.csv"

#get_vienna_stations_from_url(URL_STATIONS_RBL)
stations = get_vienna_stations_local("data/vienna_stations_all_raw_rbl.csv")
print(stations.columns)

