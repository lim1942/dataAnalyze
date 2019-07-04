import sys
import os
# if sys.getsizeof(str_message) >8*1024*1000:
from odps import ODPS

# INSERT INTO TABLE zqh_auth_20190620(message,pt)
# SELECT *
# FROM fengdun_model_contrast
# WHERE pt BETWEEN '2019_06_19_00_00' AND '2019_06_20_00_00'
# AND GET_JSON_OBJECT(message,'$.appName')='攒qh' AND GET_JSON_OBJECT(message,'$.ver')


# SELECT *
# FROM
# fengdun_model_contrast
# WHERE
# pt = MAX_PT('fengdun_model_contrast')
# LIMIT
# 10

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
    def creat_table(self,table_name,columns,if_not_exists=True,lifecycle=28):
        # table_name: 表名
        # columns :  ('num bigint, num2 double', 'pt string') 字段和分组的元组
        # if_not_exists:True   不存在才创建
        # lifecycle:28   生命周期
        # return 返回表对象
        return self.odps.create_table(table_name,columns,if_not_exists=if_not_exists,lifecycle=lifecycle)

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


    # 向一张表写入多条数据，
    def write_to_table(self,table,records_list,partition=None,**kw):
        # table: 表对象
        # records_list:表数据对象的列表
        #多个block能多线程写入
        blocks = [block for block in range(len(records_list))]
        with table.open_writer(partition=partition, blocks=blocks, **kw) as writer:
            for block, records in zip(blocks, records_list):
                writer.write(block,records)


    # ===========执行sql=========
    # sql查询
    def select_sql(self,sql):
        # return: 查询结果的迭代对象
        with self.odps.execute_sql(sql).open_reader() as reader:
            return reader
    # 其它sql
    def insert_sql(self,sql):
        # retrun: 插入的结果
        return self.odps.execute_sql(sql)



if __name__ == "__main__":
    obj = myOdps(access_id=os.getenv("ALIAK"),secret_access_key=os.getenv("ALIAKPWD"),project="shanghai")
    # print(obj.get_a_table("zqh_repay_0614_0620"))

    sql = """SELECT count(*)
    FROM
    zqh_repay_all
    """

    res = obj.select_sql(sql)
    for i in res:
        print(i)