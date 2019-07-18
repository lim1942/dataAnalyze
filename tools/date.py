import os
import pandas as pd

from tools.IOuitls import download

# 初始化日期文件
date_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'date.txt')
if not os.path.exists(date_file):
    with open(date_file, 'w') as f:
        f.write("{}")

# 读取日期文件到字典
with open(date_file) as f:
    date_dict_all = eval(f.read())

# 访问日期接口
def get_res(url):
    while True:
        resp = download(url, verify=False, timeout=2)
        res = resp.text
        if res in "012":
            return res

# 获取某一天是节假日还是工作日双休日
def get_date_type(date):
    """
    获取某一天是工作日（0），双休日（1），节假日（2）
    date: a datetime obj
    """
    date_str = date.strftime("%Y%m%d")
    year = date.year
    year_str = str(year)
    date_dict = date_dict_all.get(year_str,{})
    if not date_dict:
        for day in pd.date_range(year_str,str(year+1),freq='d'):
            day_str = day.strftime("%Y%m%d")
            url = f"http://tool.bitefu.net/jiari/?d={day_str}"
            res = get_res(url)
            date_dict[day_str] = res
            print(f"{day_str} == {res}")
        # 刷新本地日期文件
        date_dict_all[year_str] = date_dict
        with open(date_file,'w') as f:
            f.write(str(date_dict_all))
    # 返回当天的日期类型
    date_type = date_dict[date_str]
    return date_type


if __name__ == "__main__":
    import datetime
    print(get_date_type(datetime.datetime.now()))
