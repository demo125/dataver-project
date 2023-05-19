import json
from typing import Union
import os
import dvc
from dvc.api import DVCFileSystem
import shutil
from dataclasses import dataclass
DATAVER_DIR = '/home/mde/other/dataver-collection/dataver/'
DATAVER_DIR = 'https://github.com/demo125/dataver'
fs = DVCFileSystem(DATAVER_DIR)
DATAVER_DATASTORE_DIR = 'datastore/data'

@dataclass
class DataObject:

    dvc_object_path: str
    rev: None|str
    dataset_object_path: str

class Dataset:

    data_obejcts: list[DataObject]
    repo_dir: str

    def __init__(self, repo_dir: str) -> None:
        self.repo_dir = repo_dir
        self.data_obejcts = []

    def load_dataset(self):
        with open('datasets/dataset1.dataver.json') as f:
            dataset: list[DataObject] = json.load(f)
            for d in dataset:
                do = DataObject(dvc_object_path=)
                self.data_obejcts.append(do)


fs_get_file = lambda file_path, output_path, rev: fs.get_file(os.path.join(DATAVER_DATASTORE_DIR, file_path), output_path)
fs_get_folder = lambda folder_path, output_path, rev: fs.get(os.path.join(DATAVER_DATASTORE_DIR, folder_path), output_path, recursive=True)

fs.get_file(os.path.join(DATAVER_DATASTORE_DIR, 'file1.txt'), 'qwe2.txt')
fs = DVCFileSystem(DATAVER_DIR, rev='8e26a141440a0507b41553c94062046b0a6e750c')
fs.get_file(os.path.join(DATAVER_DATASTORE_DIR, 'file1.txt'), 'qwe1.txt')
exit()
MYDATA_DIR = 'mydata'


def get_dvc_object(object_path: str, output_path:str, revision:Union[str, None]):
    
    dir_path = os.path.dirname(output_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    fs_get_file(object_path, output_path, revision)
   

with open('datasets/dataset1.dataver.json') as f:
    dataset = json.load(f)
    
    if os.path.isdir(MYDATA_DIR): shutil.rmtree(MYDATA_DIR)
    os.mkdir(MYDATA_DIR)

    for d in dataset:
        dvc_object_path = d['dvc_object_path']
        revision = d['rev'] if 'rev' in d else None
        mydata_object_path = os.path.join(MYDATA_DIR, d['dataset_object_path'])
        get_dvc_object(dvc_object_path, mydata_object_path, revision)
