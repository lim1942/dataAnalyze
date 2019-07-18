import os
import sys
import time
import requests

from odps import ODPS


class myOdps:

    # 初始化一个odps连接对象
    def __init__(self,access_id,secret_access_key,project):
        self.odps = ODPS(access_id=access_id,secret_access_key=secret_access_key,
                         project=project,end_point="http://service.odps.aliyun.com/api")

    # 获取所有表名
    def get_all_tabel(self):
        # return 返回所有的表名
        table_name = []
        for table in self.odps.list_tables():
            table_name.append(table.name)
        return table_name

    # 创建一张表
    def creat_table(self,table_name,columns=None,if_not_exists=True):
        # table_name: 表名
        # columns :  ('num bigint, num2 double', 'pt string') 字段和分组的元组
        # if_not_exists:True   不存在才创建
        # lifecycle:28   生命周期
        # return 返回表对象
        try:
            return self.odps.create_table(table_name,columns,if_not_exists=if_not_exists)
        except:
            return self.odps.get_table(table_name)

    # 通过表名直接获取一张表
    def get_a_table(self,table_name):
        # table_name: 表名
        # return 返回表对象
        return self.odps.get_table(table_name)

    # 删除一张表
    def drop_a_table(self,table_name):
        # table_name: 表名
        # return 返回表删除结果
        return self.odps.delete_table(table_name)

    # 获取一张表的所有分区
    def get_partitions(self,table):
        # table:表对象
        # return: 表的所有分区
        partitions = []
        for partition in table.partitions:
            partitions.append(partition.name)
        return partitions


    # ============= 数据上传 ============
    # 上传csv到odps并创建表，csv必须要有表头
    def uploadCSV(self,csvFilename,tableName,sep=","):
        """
        :param csvFilename: 传入本地csv的路径，必须要有表头
        :param tableName:  上传到odps时的表名
        :param sep:   csv的分隔符
        """
        print("start upload ...\n")
        f = open(csvFilename,encoding='utf-8')
        lines = f.readlines()
        _len = len(lines) - 2
        f.close()
        head = lines[0].strip('\n').split(sep)
        pt_time = time.strftime("%Y-%m-%d")
        head.append("pt")
        head_list = [f"{x} string " for x in head]
        head_line = ', '.join(head_list)
        table = self.creat_table(tableName,head_line)
        with table.open_writer(partition=None) as writer:
            for index,line in enumerate(lines[1:]):
                print(f"{index} /{_len} in {tableName}  ...")
                item = line.strip('\n').split(sep)
                item.append(pt_time)
                writer.write(item)
        print("\n\n upload finish ...")


    # 上传的过程中并进行下载，下载完再上传完整的数据,数据行的坐标为1的字段为下载地址
    def downloaAndUp(self,csvFilename,tableName,sep=","):
        """
        :param csvFilename: 传入本地csv的路径，必须要有表头
        :param tableName:  上传到odps时的表名
        :param sep:   csv的分隔符
        """
        def download(url, meth="get"):
            if meth == "get":
                while 1:
                    try:
                        resp = requests.get(url)
                        return resp
                    except:
                        print("error in download")
                        time.sleep(1)

        print("start upload ...\n")
        f = open(csvFilename,encoding='utf-8')
        lines = f.readlines()
        _len = len(lines) - 2
        f.close()
        head = lines[0].strip('\n').split(sep)
        pt_time = time.strftime("%Y-%m-%d")
        head.append("pt")
        head_list = [f"{x} string " for x in head]
        head_line = ', '.join(head_list)
        table = self.creat_table(tableName,head_line)
        with table.open_writer(partition=None) as writer:
            for index, line in enumerate(lines[1:]):
                print(f"{index} /{_len} in {tableName}  ...")
                item = line.strip('\n').split(sep)
                item.append(pt_time)
                resp = download(item[1])
                resp.encoding = "utf-8"
                data = resp.text
                if sys.getsizeof(data) <= 8 * 1024 * 1000:
                    item[1] = data
                    writer.write(item)
                else:
                    print(f"failed in {item[0]}")
        print("\n\n upload finish ...")


    # ===========执行sql=========
    # sql查询
    def select_sql(self,sql):
        # return: 查询结果的迭代对象
        with self.odps.execute_sql(sql).open_reader() as reader:
            return reader




if __name__ == "__main__":
    obj = myOdps(access_id=os.getenv("ALIAK"),secret_access_key=os.getenv("ALIAKPWD"),project="shanghai")

    # sql = """SELECT count(*)
    # FROM
    # zqh_repay_all
    # """
    #
    # res = obj.select_sql(sql)
    # for i in res:
    #     print(i)
    obj.uploadCSV("/Users/apple/Documents/project/analyze/shutdownAnalyze/all_csv","shutdown_analyze")