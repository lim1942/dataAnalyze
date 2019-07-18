import pandas as pd
import requests
import timeout_decorator
from multiprocessing import Lock
lock = Lock()

def write_result(filename,result):
    lock.acquire()
    line = str(result)
    with open(filename,'a',encoding='utf-8') as f:
        f.write(line +'\n')
    lock.release()


def json2csv(path):
    csv_name = path.split('.')[0] + '.csv'
    df = pd.read_json(path,lines=True)
    df.to_csv(csv_name,index=False)
    print('finish ..')


@timeout_decorator.timeout(2)
def downloads(url,method='get',**kwargs):
    while 1:
        try:
            resp = requests.request(method,url,**kwargs)
            if resp.status_code ==200:
                resp.encoding = 'utf-8'
                return resp
        except:
            print('download error retry again !!!')


def download(url,method='get',**kwargs):
    while 1:
        try:
            resp = requests.request(method,url,**kwargs)
            if resp.status_code ==200:
                resp.encoding = 'utf-8'
                return resp
        except:
            print('download error retry again !!!')


