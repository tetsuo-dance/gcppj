# -*- coding: utf-8 -*-
import subprocess
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('setting.ini')

section1 = 'setting'
data_set_id = config.get(section1,'data_set_id')
table_id = config.get(section1,'table_id')
file_name = config.get(section1,'file_name')
partition = config.get(section1,'partition')
source_dir = config.get(section1,'source_dir')
#ファイル名の組み立て
file_full_path  = source_dir + file_name
print file_full_path
bq_query = 'bq load %s.%s %s name:string,gender:string,count:integer' %(data_set_id,table_id,file_full_path)
print bq_query
subprocess.call(bq_query, shell=True)


