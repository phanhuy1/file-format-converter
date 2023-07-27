import glob
import os
import json
import re
import pandas as pd


def get_columns_name(schemas, table, sorting_key='column_position'):
    return list(map(lambda column: column.get('column_name'), sorted(schemas.get(table), key=lambda col: col[sorting_key])))

def read_csv(file, schemas):
    file_path = re.split('[/\\\]', file)
    ds_name = file_path[-2]
    file_name = file_path[-1]
    columns = get_columns_name(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df


def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True)
    
def file_converter(src_file_names, tgt_base_dir, ds_name):
    schemas = json.load(open(f'{src_file_names}/schemas.json'))
    files = glob.glob(f'{src_file_names}/{ds_name}/part-*')
    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tgt_base_dir, ds_name, file_name)
        
def process_files(ds_names=None):
    src_base_dir = 'data/retail_db/'
    tgt_base_dir = 'data/retail_db_json'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        print(f'Processing {ds_name}')
        file_converter(src_base_dir, tgt_base_dir, ds_name)