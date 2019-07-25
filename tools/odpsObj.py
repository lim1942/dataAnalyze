import sys
import requests
import pandas as pd

from odps import ODPS
from config import config
from odps.models import Schema, Column, Partition


def download(url,method='get',**kwargs):
    """
    无限时的下载器
    """
    while 1:
        try:
            resp = requests.request(method,url,**kwargs)
            if resp.status_code ==200:
                resp.encoding = 'utf-8'
                return resp
        except:
            print('download error retry again !!!')



class myOdps:

    # 初始化一个odps连接对象
    def __init__(self, access_id, secret_access_key, project):
        self.odps = ODPS(access_id=access_id, secret_access_key=secret_access_key,
                         project=project, end_point="http://service.odps.aliyun.com/api")

    # 获取所有表名
    def get_all_tabel(self):
        # return 返回所有的表名
        table_name = []
        for table in self.odps.list_tables():
            table_name.append(table.name)
        return table_name

    # 创建一张表
    def creat_table(self, table_name, columns=None, if_not_exists=True):
        # table_name: 表名
        # columns :  ('num bigint, num2 double', 'pt string') 字段和分组的元组
        # if_not_exists:True   不存在才创建
        # lifecycle:28   生命周期
        # return 返回表对象
        try:
            return self.odps.create_table(table_name, columns, if_not_exists=if_not_exists)
        except:
            return self.odps.get_table(table_name)

    # 通过表名直接获取一张表
    def get_a_table(self, table_name):
        # table_name: 表名
        # return 返回表对象
        return self.odps.get_table(table_name)

    # 删除一张表
    def drop_a_table(self, table_name):
        # table_name: 表名
        # return 返回表删除结果
        return self.odps.delete_table(table_name)

    # 获取一张表的所有分区
    def get_partitions(self, table):
        # table:表对象
        # return: 表的所有分区
        partitions = []
        for partition in table.partitions:
            partitions.append(partition.name)
        return partitions

     # ============= 数据上传 ============
    # 上传csv到odps并创建表，csv必须要有表头
    def uploadCSV(self, csvFilename, tableName, sep=",",pt=None):
        """
        :param csvFilename: 传入本地csv的路径，必须要有表头
        :param tableName:  上传到odps时的表名
        :param sep:   csv的分隔符
        :param pt:   是否创建分区
        """
        print("start upload ...\n")
        df = pd.read_csv(csvFilename,sep=sep)
        shape0 = df.shape[0]
        columns = [Column(name=f"{x}", type='string', comment='the column') for x in df.columns]

        if pt:
            partitions = [Partition(name='pt', type='string', comment='the partition')]
            schema = Schema(columns=columns, partitions=partitions)
            table = self.creat_table(tableName, schema)
            table.create_partition(f"pt={pt}", if_not_exists=True)
            table_columns = [i.name for i in table.schema.columns]
            with table.open_writer(partition=f"pt={pt}") as writer:
                for index in df.index:
                    print(f"{index+1}/{shape0} in {tableName}  ...")
                    item_dict = dict(df.loc[index])
                    item = []
                    for field in table_columns[:-1]:
                        item.append(item_dict.get(field,''))
                    item.append(pt)
                    writer.write(item)
        else:
            schema = Schema(columns=columns)
            table = self.creat_table(tableName, schema)
            table_columns = [i.name for i in table.schema.columns]
            with table.open_writer(partition=None) as writer:
                for index in df.index:
                    print(f"{index+1}/{shape0} in {tableName}  ...")
                    item_dict = dict(df.loc[index])
                    item = []
                    for field in table_columns[:-1]:
                        item.append(item_dict.get(field,''))
                    writer.write(item)
        print("\n\n upload finish ...")


    # 上传的过程中并进行下载，下载完再上传完整的数据,数据行的坐标为1的字段为下载地址
    def downloaAndUp(self,csvFilename,tableName,sep=",",urlIndex=1,pt=None):

        """
        :param csvFilename: 传入本地csv的路径，必须要有表头
        :param tableName:  上传到odps时的表名
        :param sep:   csv的分隔符
        :param urlIndex: url字段的坐标位置
        """
        print("start upload ...\n")
        f = open(csvFilename, encoding='utf-8')
        first_line = f.readlines(1)[0].strip('\n').split(sep)
        columns = [Column(name=f"{x}", type='string', comment='the column') for x in first_line]

        if pt:
            partitions = [Partition(name='pt', type='string', comment='the partition')]
            schema = Schema(columns=columns, partitions=partitions)
            table = self.creat_table(tableName, schema)
            table.create_partition(f"pt={pt}", if_not_exists=True)
            with table.open_writer(partition=f"pt={pt}") as writer:
                for index, line in enumerate(f):
                    print(f"{index} in {tableName}  ...")
                    item = line.strip('\n').split(sep)
                    item.append(pt)
                    resp = download(item[urlIndex])
                    data = resp.text
                    if sys.getsizeof(data) <= 8 * 1024 * 1000:
                        item[urlIndex] = data
                    else:
                        print(f"failed in {item[0]}")
                    writer.write(item)
        else:
            schema = Schema(columns=columns)
            table = self.creat_table(tableName, schema)
            with table.open_writer(partition=None) as writer:
                for index, line in enumerate(f):
                    print(f"{index}  in {tableName}  ...")
                    item = line.strip('\n').split(sep)
                    resp = download(item[urlIndex])
                    data = resp.text
                    if sys.getsizeof(data) <= 8 * 1024 * 1000:
                        item[urlIndex] = data
                    else:
                        print(f"failed in {item[0]}")
                    writer.write(item)
        print("\n\n upload finish ...")
        f.close()


    # ===========执行sql=========
    # sql查询
    def select_sql(self, sql):
        # return: 查询结果的迭代对象
        with self.odps.execute_sql(sql).open_reader() as reader:
            return reader




obj = myOdps(access_id=os.getenv("ALIAK"),secret_access_key=os.getenv("ALIAKPWD"),project="shanghai")
if __name__ == "__main__":

    sql = """SELECT *
    FROM jnn_repay where pt > "2018_05_09"
    """
    #
    _l = list()
    import pandas as pd
    res = obj.select_sql(sql)
    for i in res:
        res = {}
        res['mobile'] = i['phone']
        res['loan_end_time'] = i['loan_end_time']
        res['repay_time'] = i['repay_time']
        _l.append(res)

    df = pd.DataFrame(_l)
    df.to_csv('temp.csv',index=False)
    # obj.uploadCSV("/Users/apple/Documents/project/analyze/shutdownAnalyze/all_csv","shutdown_analyze")