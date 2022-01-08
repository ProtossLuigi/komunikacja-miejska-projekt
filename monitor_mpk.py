from os.path import exists
from time import sleep
import requests
from datetime import datetime
import pytz
import pandas as pd
from time import time

LOCATIONS_URL = 'https://mpk.wroc.pl/bus_position'
SAVE_PATH = 'data/locations_data.csv'
INTERVAL = 10
DUMP_EVERY = 10

file_initialized = None

def get_routes():
    return list(pd.read_csv('data/routes.txt')['route_id'])

def request_data(routes):
    resp = requests.post(
        LOCATIONS_URL,
        data={'busList[][]': routes}
    )
    return resp.json(), datetime.strptime(resp.headers['Date'], '%a, %d %b %Y %H:%M:%S GMT').replace(tzinfo=pytz.utc)

def dump_data(df: pd.DataFrame):
    global file_initialized
    if file_initialized:
        df.to_csv(SAVE_PATH, mode='a', index=False, header=False)
    else:
        df.to_csv(SAVE_PATH, mode='w', index=False, header=True)
        file_initialized = True

def request_loop(routes):
    counter = 0
    df = None
    start_time = time()
    try:
        while True:
            loop_start = time()
            data, t = request_data(routes)
            new_df = pd.DataFrame(data)
            new_df['date'] = t
            if counter == 0:
                df = new_df
            else:
                df = pd.concat((df, new_df), ignore_index=True)
            counter += 1
            if counter == DUMP_EVERY:
                dump_data(df)
                counter = 0
                df = None
            sleep(INTERVAL - (time() - loop_start))
    except KeyboardInterrupt:
        print(f'Script was running for {time() - start_time:.0f} seconds.')
        if not df is None:
            dump_data(df)

def main():
    global file_initialized
    if exists(SAVE_PATH):
        file_initialized = True
    routes = get_routes()
    request_loop(routes)

if __name__ == '__main__':
    main()
