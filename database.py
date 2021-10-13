from pymongo import MongoClient
import glob
import os
import pandas as pd
import shutil
import sys
from datetime import datetime as dt, timedelta


def query_data(db, district, date):
    return db[district].find_one({"_id": date})

def get_latest_data(district='HCM', skip_missing = False):
    try:
        client = MongoClient("mongodb+srv://thesisbecaked:thesisbecaked@thesis.cojlj.mongodb.net")
        db = client['daily-data']
        latest_date = db.auxiliary.find_one({"type": "latest_date"}, {"latest_date": 1})['latest_date']
        if skip_missing:
            return query_data(db, district, latest_date)
        is_exist = db[district].find_one({"_id": latest_date}, {"_id": 1})
        if is_exist is not None:
            return query_data(db, district, latest_date)
        last_date = list(db[district].find({}, {"_id": 1}).sort("_id", -1))[0]['_id']
        
        data = query_data(db, district, last_date)
        client.close()
        return data
    except:
        return None

def get_daily_latest_statistics():
    client = MongoClient("mongodb+srv://thesisbecaked:thesisbecaked@thesis.cojlj.mongodb.net")
    db = client['daily-data']
    latest_date = db.auxiliary.find_one({"type": "latest_date"}, {"latest_date": 1})['latest_date']
    tmp = latest_date.split('.')
    month, date = int(tmp[0]), int(tmp[1])
    pre_date = dt.strptime(f'{date}/{month}/21', '%d/%m/%y') - timedelta(days = 1)
    curr_cum_date = f'{pre_date.month}.{pre_date.day}'
    data = db.cum_data.find_one({"_id": curr_cum_date})
    skips = ['cum_data', 'auxiliary']
    districts = db.list_collection_names()
    rv = {}
    rv['date'] = curr_cum_date
    rv['data'] = {}
    for district in districts:
        if district in skips: continue
        result = db[district].find_one({"_id": latest_date})
        I, R, D = None, None, None
        if result is not None:
            result = result['data']
            try:
                D = result['D']['real'][-1] - result['D']['real'][-2]
            except:
                pass
            try:
                R = result['R']['real'][-1] - result['R']['real'][-2]
            except:
                pass
            try:
                I = result['I']['real'][-1]
            except:
                pass
        acc_I = data['I'].get(district, None)
        acc_R = data['R'].get(district, None)
        acc_D = data['D'].get(district, None)
        rv['data'][district] = {
            'I': {'New': I, 'Total': acc_I},
            'R': {'New': R, 'Total': acc_R},
            'D': {'New': D, 'Total': acc_D}
        }
    client.close()
    return rv

#test
if __name__ == '__main__':
    # if len(sys.argv) > 1:
    #     folder_name = sys.argv[1]
    #     remove_v2_path(folder_name)

    # client = MongoClient("mongodb+srv://thesisbecaked:thesisbecaked@thesis.cojlj.mongodb.net")
    # # client = MongoClient('mongodb://localhost:27017/')
    # db = client['daily-data']
    # if len(sys.argv) > 1:
    #     insert_new_data(db, folder_name)
    # print(query_data(db, "HCM", "10.12"))
    # client.close()
    print(get_latest_data('QUAN 3'))
