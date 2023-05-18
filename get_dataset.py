import json
import os
import dvc
from dvc.api import DVCFileSystem
import shutil

DATAVER_DIR = '/home/mde/other/dataver-collection/dataver/'
DATAVER_DIR = 'https://github.com/demo125/dataver'
fs = DVCFileSystem(DATAVER_DIR)
DATAVER_DATASTORE_DIR = 'datastore'

fs_get_file = lambda file_path, output_path: fs.get(os.path.join(DATAVER_DATASTORE_DIR, file_path), output_path)
fs_get_folder = lambda folder_path, output_path: fs.get(os.path.join(DATAVER_DATASTORE_DIR, folder_path), output_path, recursive=True)

MYDATA_DIR = 'mydata'


def get_dvc_object(object_path: str, output_path:str):
    
    dir_path = os.path.dirname(output_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    fs_get_file(object_path, output_path)
   

with open('datasets/dataset1.dataver.json') as f:
    dataset = json.load(f)
    
    if os.path.isdir(MYDATA_DIR): shutil.rmtree(MYDATA_DIR)
    os.mkdir(MYDATA_DIR)

    for d in dataset:
        dvc_object_path = d['dvc_object_path']
        mydata_object_path = os.path.join(MYDATA_DIR, d['dataset_object_path'])
        get_dvc_object(dvc_object_path, mydata_object_path)
