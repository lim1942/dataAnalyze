import os
os.chdir('/Users/apple/Desktop/数据分析/strategy/20190708')
import pandas as pd

more_record=pd.read_csv('strategy_judge_convert.csv')
more_record_nomobile=more_record.drop(['mobile'],axis=1)

head=list(more_record_nomobile.columns)
head.remove('label')

# 策略分析
more_record_nomobile_bad=more_record_nomobile[more_record_nomobile.label==1]#总的坏用户
more_record_nomobile_good=more_record_nomobile[more_record_nomobile.label==0]#总的好用户
total_bad_cnt=more_record_nomobile_bad.shape[0]#总的坏用户数
total_good_cnt=more_record_nomobile_good.shape[0]#总的好用户数
total_cnt_list=[]#有表现样本数
target_list=[]#命中用户数
target_rate_list=[]#策略命中率
target_bad_rate_list=[]#策略命中的坏用户占比
target_good_rate_list=[]#策略误杀的好用户占比



for i in head:
    total_cnt=more_record_nomobile[more_record_nomobile[i].notnull()].shape[0]#有表现样本数
    pass_cnt=more_record_nomobile[i].sum()#策略通过人数
    pass_cnt_bad=more_record_nomobile_bad[i].sum()#策略通过的坏用户数
    target=total_cnt-pass_cnt#策略命中用户数
    target_rate=(total_cnt-pass_cnt)/total_cnt #策略命中率
    
    temp=more_record_nomobile[more_record_nomobile[i]==0]#策略命中的记录
    target_bad_cnt=temp[temp.label==1].shape[0]#策略命中坏用户数
    target_bad_rate=target_bad_cnt/target if target else 0#策略命中坏用户占比
    
    target_good_cnt=temp[temp.label==0].shape[0]#策略命中的好用户数
    target_good_rate=target_good_cnt/target if target else 0#策略误杀的好用户占比
    total_cnt_list.append(total_cnt)
    target_list.append(target)
    target_rate_list.append(target_rate)
    target_bad_rate_list.append(target_bad_rate)
    target_good_rate_list.append(target_good_rate)


more_result=pd.DataFrame()
more_result['strategyname']=head
more_result['total_cnt']=total_cnt_list
more_result['target']=target_list
more_result['target_rate']=target_rate_list
more_result['target_bad_rate']=target_bad_rate_list
more_result['target_good_rate']=target_good_rate_list


