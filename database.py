from pymongo import MongoClient
import glob
import os
import pandas as pd
import shutil
import sys
from datetime import datetime as dt, timedelta
import json

def remove_v2_path(folder_name = 'data'):
    all_csv_paths = glob.glob(f'{folder_name}/*/v2/*/*/*.csv')
    for path in all_csv_paths:
        new_path = path.replace('/v2', '')
        new_dir = new_path.rsplit('/', 1)[0]
        if not os.path.exists(new_dir):
            os.system(f'mkdir -p "{new_dir}"')
        os.replace(path, new_path)

def prepare_all_data(folder_name = 'data'):
    all_csv_paths = glob.glob(f'{folder_name}/*/*/*/*.csv')
    districts = ['BINH CHANH', 'BINH TAN', 'BINH THANH', 'CAN GIO', 'CU CHI', 'GO VAP', 'HCM', 'HOC MON', 'NHA BE', 'PHU NHUAN'] + [f'QUAN {i}' for i in [1, 3, 4, 5, 6, 7, 8, 10, 11, 12]] + ['TAN BINH', 'TAN PHU', 'THU DUC']
    data = {}
    for district in districts:
        paths = list(filter(lambda x: district in x, all_csv_paths))
        data[district] = {}
        for path in paths:
            coms = path.split(os.path.sep)
            date = coms[1]
            case = coms[-2]
            if os.stat(path).st_size == 0: continue
            df = pd.read_csv(path)
            name = coms[-1]
            if 'infectious' in name:
                compartment = 'I'
            elif 'recovered' in name:
                compartment = 'R'
            elif 'deceased' in name:
                compartment = 'D'
            elif 'critical' in name or 'ecmo' in name:
                compartment = 'C'
            if date not in data[district]:
                data[district][date] = {}
                data[district][date][compartment] = {}
                data[district][date][compartment][case] = df['Predict'].values.tolist()
                if 'Real' in df.columns:
                    data[district][date][compartment]['real'] = df['Real'].dropna(axis = 0).values.tolist()
                data[district][date]['dates'] = df['Date'].values.tolist()
            else:
                if compartment not in data[district][date]:
                    data[district][date][compartment] = {}
                if 'Real' in df.columns and 'real' not in data[district][date][compartment]:
                    data[district][date][compartment]['real'] = df['Real'].dropna(axis = 0).values.tolist()
                data[district][date][compartment][case] = df['Predict'].values.tolist()
    return data
def convert_into_mongo_format(data):
    mongo_data = {}
    for name, value in data.items():
        mongo_data[name] = []
        for date, _data in value.items():
            mongo_data[name].append({'_id': date, 'data': _data})
    return mongo_data

def insert_new_data(db, folder_name):
    data = prepare_all_data(folder_name)
    mongo_data = convert_into_mongo_format(data)
    for name, value in mongo_data.items():
        if len(value) == 0:
            print(f'MISSED {name}')
            continue
        db[name].insert_many(value)

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
        client.close()
        return query_data(db, district, last_date)
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
        if result is None: continue
        result = result['data']
        try:
            D = result['D']['real'][-1] - result['D']['real'][-2]
        except:
            D = None
        try:
            R = result['R']['real'][-1] - result['R']['real'][-2]
        except:
            R = None
        try:
            I = result['I']['real'][-1]
        except:
            I = None
        try:
            acc_I = data['I'].get(district, None)
            acc_R = data['R'].get(district, None)
            acc_D = data['D'].get(district, None)
        except:
            acc_I = acc_R = acc_D = 0
        rv['data'][district] = {
            'I': {'New': I, 'Total': acc_I},
            'R': {'New': R, 'Total': acc_R},
            'D': {'New': D, 'Total': acc_D}
        }
    client.close()
    return rv

#testBACKUP_DATA_PATH="./backup"
if __name__ == '__main__':
    data = get_latest_data()
    summary = get_daily_latest_statistics()

    backup_data_path = os.environ.get("BACKUP_DATA_PATH", "./backup/backup_data.json")
    backup_summary_path = os.environ.get("BACKUP_SUMMARY_PATH", "./backup/backup_summary.json")
    with open(backup_data_path,'w') as json_file:
        json.dump(data,json_file)
    with open(backup_summary_path,'w') as json_file:
        json.dump(summary,json_file)
