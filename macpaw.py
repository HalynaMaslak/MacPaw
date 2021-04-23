# -*- coding: utf-8 -*-
from requests import get
import json
from collections import namedtuple
from datetime import datetime
from string import punctuation
import sqlite3 as sql
import sys

url = 'https://data-engineering-interns.macpaw.io/'
data_file = 'files_list.data'
file_list = get(url+data_file)
if file_list.status_code == 403:
    print(data_file, 'not found')
    sys.exit()

#database specifications
songs_format = [('artist_name', 'varchar'), ("title","varchar"),("year","integer"),("release","varchar"),("ingestion_time","datetime")]
movies_format = [("original_title","varchar"),("original_language","varchar"),("budget","integer"),("is_adult","boolean"),("release_date","date"),("original_title_normalized","varchar")]
apps_format = [("name","varchar"),("genre","varchar"),("rating","float"),("version","varchar"),('size_bytes','integer'),('is_awesome','boolean')]

songs, movies, apps = [], [], []
Song = namedtuple('song', [name for name,column_type in songs_format])
Movie = namedtuple('movie', [name for name,column_type in movies_format])
App = namedtuple('app', [name for name,column_type in apps_format])

new_db = False
prev_files = []
try:
    with open("prev_files.json") as f:
        prev_files = json.load(f)
        unique_files = set(file_list.text.split('\n')) - set(prev_files)
except (json.decoder.JSONDecodeError, FileNotFoundError):
    new_db = True
    unique_files = set(file_list.text.split('\n'))
    
for file in unique_files:
    print('Processing', file)
    data = get(url+file)
    if file_list.status_code == 403:
        print(data_file, 'not found')
        continue
    data_dict = json.loads(data.text)
    for item in data_dict:

        try:
            if item['type'] == 'song':
                songs.append(Song(**item['data'], ingestion_time=datetime.now()))
            elif item['type'] == 'movie':
                norm = ''.join([ch.lower() for ch in item['data']['original_title'] if ch not in punctuation]).replace(' ', '_')
                movies.append(Movie(**item['data'], original_title_normalized=norm))
            elif item['type'] == 'app':
                apps.append(App(**item['data'], is_awesome = True if item['data']['rating']>=4.5 else False))
        except TypeError:
            print('Not enough data for', file, item)
        
with sql.connect('macpaw.db') as con:
    cur = con.cursor()
    l = [['songs', songs, songs_format], ['movies', movies, movies_format], ['apps', apps, apps_format]]
    for table, items, db_format in l:
        if new_db:
            try:
                cur.execute('DROP TABLE ' + table)
            except sql.OperationalError:
                print('Table not found')
            cur.execute('CREATE TABLE {} ({})'.format(table, ','.join([item[0] + ' ' + item[1] for item in db_format])))
        for item in items:
            text = 'INSERT INTO {} VALUES(:{})'.format(table, ',:'.join(item._fields))
            cur.execute(text, item._asdict())
 
with open('prev_files.json', 'w') as f:
    json.dump(list(unique_files.union(prev_files)), f)



