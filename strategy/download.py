import pandas as pd
import requests
import json
import time

import sys
sys.path.append("/Users/apple/Documents/project/analyze/tools")
from multiWorker import ProcessPool
from findRegion import findAttribution,findRegion

# 设置打印展示输出
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)
# 数据获取
df = pd.read_csv("/Users/apple/Documents/project/analyze/strategy/dataset/less_record.csv")
relative = ['爸','爹','妈','娘','伯','婶','叔','姨','舅','姑','侄','孙','媳','婿','姥','阿爸','阿伯','阿爹','阿妈','阿娘','阿婶','阿叔','阿姨','爸爸','表弟','表哥','表姑','表姐','表妹','表叔','表兄弟','表侄','表姊妹','伯伯','伯父','伯母','伯叔','曾外祖父','曾外祖母','大伯','大姑','大姑子','大舅子','大妈','大娘','大爷','大姨','弟弟','弟妇','弟妹','弟媳','二弟','儿媳','儿子','父亲','干爹','干妈','干亲家','高祖父','高祖母','哥哥','公公','姑夫','姑姑','姑母','姑奶奶','姑爷','继父','继母','继娘','继爷','囝囝','姐夫','姐姐','襟弟','襟兄','妗子','舅父','舅舅','舅姥','舅妈','舅母','舅母子','老弟','老哥','老公','老姐','姥姥','老妹','老奶奶','老婆','老挑','老爷','姥爷','老爷爷','连襟','妈妈','妈咪','妹夫','妹妹','母亲','奶奶','囡囡','内弟','内兄','娘舅','女儿','女婿','旁系','婆婆','妻妹子','妻子','亲人','三弟','嫂子','婶母','婶婶','婶子','叔父','叔叔','叔丈母','叔丈人','孙女','孙女婿','孙媳妇','孙子','堂弟','堂哥','堂姐','堂妹','堂兄弟','堂姊妹','外公','外婆','外甥','外甥女','外甥女婿','外甥媳妇','外孙','外孙女','外祖父','外祖母','媳妇','小姑','小姑子','小舅子','小叔','小姨','小姨妹','玄孙','血老表','养父','养母','幺爸','幺婶','爷叔','爷爷','姨夫','姨公','姨姐','姨老表','姨妈','姨妹','姨妹夫','姨母','姨叔','姻亲','幼弟','幼妹','岳父','岳母','曾孙','曾祖父','曾祖母','丈夫','丈母娘','丈人','长兄','长姊','侄女','侄女婿','侄孙','侄孙女','侄媳妇','侄子','妯娌','姊夫','祖父','祖姑母','祖母']




def judge(a,b):
    a = a.strip()
    b = b.strip()
    res = '1' if (a!='') and (b!='') and(a==b) else '0'
    return res

def TF_2_10(x):
    if x:
        return '1'
    elif x=="":
        return ""
    else:
        return '0'

def phone_handle(x):
    x = x.replace("+86","")
    x = x.replace(" ","")
    return x

def get_10(x):
    x = x.reset_index()
    if len(x["index"])>=10:
        return x["index"][:10]
    else:
        return x["index"][:]

def division(x,y):
    if (x == None or x=="") or not y:
        return ""
    else:
        return x/y

def int_compare(x,method="gt",y=None):
    try:
        int(x);int(y)
        if method == "eq":
            return  x==y
        elif method=="lt":
            return x<y
        elif method == "lte":
            return x <= y
        elif method == "gt":
            return x > y
        elif method == "gte":
            return x >= y
    except:
        return ''


def spider(url):
    while 1:
        try:
            resp = requests.get(url)
            return resp
        except:
            print("error in nerwork ...")
            time.sleep(2)


def execute(item):
    # 用于记录单个人策略判断
    result = dict()
    index = item[0]
    mobile = str(item[1])
    result["mobile"] = mobile
    print(f"第{index}/8934 个人，手机号:{mobile}")

    # 三个数据的下载
    # 通话记录数据
    operator_url = item[2]
    operator_resp = spider(operator_url)
    operator_resp.encoding = "utf-8"
    operator_json = operator_resp.json()
    # operator_json = json.loads(item[2])
    # 通讯录数据
    contacter_url = item[4]
    contacter_resp = spider(contacter_url)
    contacter_resp.encoding = "utf-8"
    contacter_json = contacter_resp.json()
    # contacter_json = json.loads(item[3])
    # 电商数据
    ecommerce_url = item[3]
    ecommerce_resp = spider(ecommerce_url)
    ecommerce_resp.encoding = "utf-8"
    ecommerce_json = ecommerce_resp.json()
    # ecommerce_json = json.loads(item[4])
    #风遁数据
    fengdun_data = item[5]
    fengdun_json = json.loads(fengdun_data)
    # fengdun_json = json.loads(item[5])


    # 进行策略通过判断，通过的字段1，不通过0
    result = ecommerce_strategy(result, ecommerce_json)
    result = operator_strategy(result, operator_json,fengdun_json)
    result = contacter_strategy(result, contacter_json,operator_json,fengdun_json)

    # # 好坏标签
    # label = item[5]
    # result['label'] = label
    # print(result)

    # 存储该人的策略判断到本地csv
    result_line = json.dumps(result) +'\n'
    with open("new/strategy_judge_no_label.txt","a",encoding="utf-8") as f1:
        f1.write(result_line)

    # # 本地缓存数据
    # data_line = '^'.join([mobile,operator_data,contacter_data,ecommerce_data,fengdun_data,label,"\n"])
    # with open("local_storage.txt","a",encoding="utf-8") as f2:
    #     f2.write(data_line)


# 通话详单的策略
def operator_strategy(result,operator_json,fengdun_json):

    # 没有实名的不过姓名和注册的身份证姓名匹配
    is_real_name = judge(fengdun_json.get("modelParamsIn",{}).get("name",""),operator_json.get("name",""))
    result['is_real_name'] = is_real_name
    # 身份证年龄小于18或大于45岁（拒绝）
    age = fengdun_json.get("modelParamsIn",{}).get("age","")
    if age:
        is_suitable_age = TF_2_10(18<= age <=45)
    else:
        is_suitable_age = ""
    result['is_suitable_age'] = is_suitable_age
    # 身份证号归属地在非准入区域（新疆、西藏）（拒绝）
    id_no = fengdun_json.get("modelParamsIn",{}).get("id_no","")
    if id_no:
        is_suitable_region =  TF_2_10(id_no[0:2] not in ["54","65"])
    else:
        is_suitable_region = ""
    result['is_suitable_region'] = is_suitable_region

    return result


# 通话详单的策略
def contacter_strategy(result,contacter_json,operator_json,fengdun_json):
    #     一.=========数据读取=========
    # 0.归属地
    attribution = findAttribution(operator_json["mobile"])
    # 1.开户时间
    open_data = pd.to_datetime(operator_json.get("open_time",time.strftime('%Y-%m-%d')))
    # 2.身份证号
    id_no = fengdun_json.get("modelParamsIn", {}).get("id_no", "")
    # 3.该人出生地
    location = findRegion(id_no)
    # 4.数据更新时间
    last_modify_time = pd.to_datetime(operator_json["last_modify_time"])
    # 5.通讯录的df
    df_contacter = pd.DataFrame(contacter_json)
    df_contacter = df_contacter.drop_duplicates(subset=["mobile"])
    df_contacter["mobile"] = df_contacter["mobile"].map(phone_handle)
    df_contacter["peer_location"] = df_contacter["mobile"].map(findAttribution)
    # 6.通话记录的df
    callRecord_con = operator_json["calls"]
    callRecord = []
    for callRecord_month in callRecord_con:
        callRecord.extend(callRecord_month.get("items", []))
    df_callRecord = pd.DataFrame(callRecord)
    df_callRecord["time"] = pd.to_datetime(df_callRecord["time"])
    df_callRecord["peer_location"] = df_callRecord["peer_number"].map(findAttribution)
    df_callRecord_7day = df_callRecord[(last_modify_time-df_callRecord["time"]).dt.days<=7]
    df_callRecord_1m = df_callRecord[(last_modify_time-df_callRecord["time"]).dt.days<=30]
    df_callRecord_2m = df_callRecord[(last_modify_time-df_callRecord["time"]).dt.days<=60]
    df_callRecord_3m = df_callRecord[(last_modify_time-df_callRecord["time"]).dt.days<=90]
    # 7.流量的df
    nets_con = operator_json["nets"]
    nets = []
    for nets_month in nets_con:
        nets.extend(nets_month.get("items", []))
    df_nets = pd.DataFrame(nets)
    if not df_nets.shape[0]:
        df_nets = pd.DataFrame(columns=['details_id', 'duration', 'fee', 'location', 'net_type', 'service_name', 'subflow', 'time'])
    df_nets["time"] = pd.to_datetime(df_nets["time"])
    df_nets_1m = df_nets[(last_modify_time-df_nets["time"]).dt.days<=30]
    df_nets_3m = df_nets[(last_modify_time-df_nets["time"]).dt.days<=90]
    # 8.短信的df
    smses_con = operator_json["smses"]
    smses = []
    for smses_month in smses_con:
        smses.extend(smses_month.get("items", []))
    df_smses = pd.DataFrame(smses)
    df_smses["peer_location"] = df_smses["peer_number"].map(findAttribution)
    df_smses["time"] = pd.to_datetime(df_smses["time"])
    df_smses_7day = df_smses[(last_modify_time-df_smses["time"]).dt.days<=7]
    df_smses_1m = df_smses[(last_modify_time-df_smses["time"]).dt.days<=30]
    df_smses_3m = df_smses[(last_modify_time-df_smses["time"]).dt.days<=90]
    # 9.账单的df
    df_bills = pd.DataFrame(operator_json["bills"])
    df_bills["bill_month"] = pd.to_datetime(df_bills["bill_month"])
    df_bills = df_bills.sort_values(by="bill_month",ascending=False).reset_index(drop=True)


    # 二. ========= 策略判断 =========
    # -------2、通讯录-------
    # A、通讯录个数30 - 600个的
    is_contacters_30_60 =  TF_2_10(30<=df_contacter.shape[0]<=600)
    result['is_contacters_30_60'] = is_contacters_30_60
    # B、通讯录里的真实手机号码要占通讯录百分之60以上（过）
    df_contacts_mobile = df_contacter[df_contacter["mobile"].str.match(r"^1[3456789]\d{9}$")]
    rate = division(df_contacts_mobile.shape[0], df_contacter.shape[0])
    is_mobile_over60 = TF_2_10(int_compare(rate,'gt',0.6))
    result['is_mobile_over60'] = is_mobile_over60
    # E、通讯录里是否存有爸爸、妈妈、姐、哥、叔叔、阿姨等亲人号码记录2个月内有无和这些人联系，如2个月内都没有通话的（拒绝）
    relative_mobile = df_contacter[df_contacter["name"].map(lambda x:any([i in x.strip() for i in relative]))]["mobile"].unique()
    df_callrecord_relative = df_callRecord_2m[df_callRecord_2m["peer_number"].isin(relative_mobile)].loc[df_callRecord_2m["duration"]>=30]
    is_no_contact_relative = TF_2_10(df_callrecord_relative.shape[0]>0)
    result['is_no_contact_relative'] = is_no_contact_relative
    # F、通讯录里的号码和用户的户籍地区进行匹配是否同个地区比例小于40%拒绝
    df_contacter_in_location = df_contacter[df_contacter["peer_location"].map(lambda x:x in location)]
    df_contacter_in_location_rate = division(df_contacter_in_location.shape[0], df_contacter.shape[0])
    is_contacter_location_rate = TF_2_10( int_compare(df_contacter_in_location_rate,'gte',0.4))
    result['is_contacter_location_rate'] = is_contacter_location_rate
    # -------3、关机次数-------
    # A、3个月内关机次数大于10的拒绝
    sl_callRecord_3m_date = df_callRecord_3m["time"].dt.strftime('%Y-%m-%d').drop_duplicates()
    sl_nets_3m_date = df_nets_3m["time"].dt.strftime('%Y-%m-%d').drop_duplicates()
    sl_smses_3m_date = df_smses_3m["time"].dt.strftime('%Y-%m-%d').drop_duplicates()
    sl_all_3m_date = sl_callRecord_3m_date.append(sl_nets_3m_date).append(sl_smses_3m_date).drop_duplicates()
    is_3m_shutdown_10 = TF_2_10(90-sl_all_3m_date.shape[0]<=10)
    result['is_3m_shutdown_10'] = is_3m_shutdown_10
    # B、1个月内关机次数大于2的拒绝
    sl_callRecord_1m_date = df_callRecord_1m["time"].dt.strftime('%Y-%m-%d').drop_duplicates()
    sl_nets_1m_date = df_nets_1m["time"].dt.strftime('%Y-%m-%d').drop_duplicates()
    sl_smses_1m_date = df_smses_1m["time"].dt.strftime('%Y-%m-%d').drop_duplicates()
    sl_all_1m_date = sl_callRecord_1m_date.append(sl_nets_1m_date).append(sl_smses_1m_date).drop_duplicates()
    is_1m_shutdown_2 = TF_2_10(30-sl_all_1m_date.shape[0]<=2)
    result['is_1m_shutdown_2'] = is_1m_shutdown_2
    # C (MJ) 用户在网时长小于6个月(拒绝）
    online_month = (pd.to_datetime(time.strftime('%Y-%m-%d')) - open_data).days/30
    is_online_month_over6 = TF_2_10(online_month>=6)
    result['is_online_month_over6'] = is_online_month_over6
    # E(MJ)当月剩余话费 < =0(拒绝）
    available_balance = operator_json.get("available_balance",'')
    is_available_balance = TF_2_10(int_compare(available_balance,'gt',0))
    result['is_available_balance'] = is_available_balance
    # G (MJ)魔杖分 < 37.5(拒绝）
    mz_score = fengdun_json.get("modelParamsIn",{}).get("mz",{}).get("mz_score","")
    is_mz_score_over_37 = TF_2_10(int_compare(mz_score,"gte",37.5))
    result['is_mz_score_over_37'] = is_mz_score_over_37
    # -------4、通话记录-------
    # A、每周通话次数
    callRecord_7day_cnt = df_callRecord_7day[df_callRecord_7day["duration"]>10].shape[0]
    result['callRecord_7day_cnt'] = str(callRecord_7day_cnt)+"_cnt"
    # B、每月通话次数
    callRecord_1m_cnt = df_callRecord_1m[df_callRecord_1m["duration"]>10].shape[0]
    result['callRecord_1m_cnt'] = str(callRecord_1m_cnt)+"_cnt"
    # C、近3个月通话次数
    callRecord_3m_cnt = df_callRecord_3m[df_callRecord_3m["duration"]>10].shape[0]
    result['callRecord_3m_cnt'] = str(callRecord_3m_cnt)+"_cnt"
    # D、近6个月通话次数
    callRecord_6m_cnt = df_callRecord[df_callRecord["duration"]>10].shape[0]
    result['callRecord_6m_cnt'] = str(callRecord_6m_cnt)+"_cnt"
    for dt,df in zip(["_7day",'_1m','_2m','_3m','_6m'],[df_callRecord_7day,df_callRecord_1m,df_callRecord_2m,df_callRecord_3m,df_callRecord]):
        # 通话次数最频繁的前10个号码
        sl_callRecord_max_10 = get_10(df["peer_number"].value_counts())
        # F、通话次数最频繁的前10个号码与通讯匹配低于2个的拒绝
        max_10_in_contacts_less2 = TF_2_10(len(set(sl_callRecord_max_10) & set(df_contacter["mobile"]))>=2)
        result['max_10_in_contacts_less2'+dt] = max_10_in_contacts_less2
        # H、通话次数最频繁的前10个号码，固定电话占20 % 以上的拒绝
        sl_callRecord_max_10_fixed = sl_callRecord_max_10[~sl_callRecord_max_10.str.match(r"^1\d{10}$")]
        sl_callRecord_max_10_fixed_rate =  division(sl_callRecord_max_10_fixed.shape[0],sl_callRecord_max_10.shape[0])
        is_max_10_fixed_rate_over20 = TF_2_10(int_compare(sl_callRecord_max_10_fixed_rate,"lt",0.2))
        result['is_max_10_fixed_rate_over20'+dt] = is_max_10_fixed_rate_over20
        # I、最频繁的前10个号码，排第10位的要有1次联系记录以上。如没有的拒绝
        if len(sl_callRecord_max_10) <10:
            is_max_10_10_over1 = "0"
        else:
            peer_number_10 = sl_callRecord_max_10[9]
            peer_number_10_cnt = df[df["peer_number"]==peer_number_10].shape[0]
            is_max_10_10_over1 = TF_2_10(peer_number_10_cnt>1)
        result['is_max_10_10_over1'+dt] = is_max_10_10_over1
        # J、接听的电话跨省的多少
        over_location_cnt = df[df["peer_location"]!=attribution].shape[0]
        result['over_location_cnt'+dt] = str(over_location_cnt) + '_cnt'
    # -------5、话费使用-------
    # A、正常人现在流量用的话费应大于通话话费
    callFee = df_callRecord["fee"].sum()
    netsFee = df_nets["fee"].sum()
    is_netsFee_over_callFee = TF_2_10(netsFee>callFee)
    result['is_netsFee_over_callFee'] = is_netsFee_over_callFee
    # B、近一个月的话费使用和上2个月话费使用做比较，如近一个月明显低于前两个月平均值一半以下的拒绝。近一个月明显高于前两个月2倍以上的拒绝
    try:
        recent_mon_fee = df_bills.loc[0]['total_fee']
        last_one_mon_fee = df_bills.loc[1]['total_fee']
        last_two_mon_fee = df_bills.loc[2]['total_fee']
        is_recent_mon_fee_in = TF_2_10((last_one_mon_fee+last_two_mon_fee)/4<=recent_mon_fee<=2*(last_one_mon_fee+last_two_mon_fee))
    except:
        is_recent_mon_fee_in = ''
    result['is_recent_mon_fee_in'] = is_recent_mon_fee_in
    # C、半年内，有少于20的就拒绝吗
    is_any_mon_over20 = TF_2_10(all(list(map(lambda x:x>=2000 ,df_bills["total_fee"]))))
    result['is_any_mon_over20'] = is_any_mon_over20
    # D、流量消费每个月做对比，每月流量消费低于10元的在我感觉就是不正常拒绝
    netsFee_over10_mon = TF_2_10(netsFee/600 >10)
    result['netsFee_over10_mon'] = netsFee_over10_mon

    #  -------6、短信回复率-------
    ## 查询短信手机号码跨区的收到有没有回复，回复率>=20%的通过（这里只限手机号码）
    # 筛选出联系对方为手机号码的记录
    df_smsesRecord_1=df_smses_7day[df_smses_7day.peer_number.str.match(r"^1[3456789]\d{9}$")]
    # 为DataFrame增加归属地列
    df_smsesRecord_1=df_smsesRecord_1.reset_index().drop(['index'],axis=1)
    # 筛选出手机号码为跨区的，计算收到的短信次数
    mobile=operator_json['mobile']
    city=findAttribution(mobile)
    df_smsesRecord_1=df_smsesRecord_1[df_smsesRecord_1.peer_location!=city]
    receive_cnt=df_smsesRecord_1[df_smsesRecord_1.send_type=='RECEIVE'].shape[0]
    # 计算收到后有回复的短信次数
    df_smsesRecord_1_receive=df_smsesRecord_1[df_smsesRecord_1.send_type=='RECEIVE'].reset_index()
    df_smsesRecord_1_receive=df_smsesRecord_1_receive.drop(['index'],axis=1)
    reply_phone=[]
    for i in df_smsesRecord_1_receive.peer_number.values.tolist():
        if i in df_smsesRecord_1_receive.peer_number.values.tolist():
            reply_phone.append(i)
    reply_cnt=len(reply_phone)
    # 计算回复率
    reply_rate=division(reply_cnt,receive_cnt)
    # 是否满足策略
    is_cross_rate_20= TF_2_10(int_compare(reply_rate,"gte",0.2))
    result['is_cross_rate_20'] = is_cross_rate_20

    # 计算收到的短信次数
    receive_cnt_total=df_smses_7day[df_smses_7day.send_type=='RECEIVE'].shape[0]
    # 计算收到后有回复的短信次数
    df_smsesRecord_receive=df_smses_7day[df_smses_7day.send_type=='RECEIVE'].reset_index()
    df_smsesRecord_receive=df_smsesRecord_receive.drop(['index'],axis=1)
    reply_phone=[]
    for i in df_smsesRecord_receive.peer_number.values.tolist():
        if i in df_smsesRecord_receive.peer_number.values.tolist():
            reply_phone.append(i)
    reply_cnt_total=len(reply_phone)
    # 计算回复率
    reply_rate_total=division(reply_cnt_total,receive_cnt_total)
    # 4.是否满足策略
    is_reply_rate_20= TF_2_10(int_compare(reply_rate_total,"gte",0.2))
    result['is_reply_rate_20'] = is_reply_rate_20

    return result

# 通话详单的策略
def ecommerce_strategy(result,ecommerce_json):
    huabeiamount = TF_2_10(int_compare(ecommerce_json.get('baseInfo',{}).get('ecommerceBaseInfo',{}).get('huabeiAmount',""),'gt', 500))
    result['huabeiamount'] = huabeiamount

    # 花呗无逾期
    huabeiStatus = TF_2_10(int_compare(ecommerce_json.get('baseInfo',{}).get('ecommerceBaseInfo',{}).get('huabeiStatus',''),'eq', 0))
    result['huabeiStatus'] = huabeiStatus

    # 电商消费三月内2笔买卖记录以上（taobaoOrders中createTime在三月内tradeStatusName==‘成功’的买卖记录笔数）
    ecommerce_json_data = ecommerce_json['baseInfo'].get('taobaoOrders',[])
    if ecommerce_json_data:
        taobaoOrders = pd.DataFrame(ecommerce_json['baseInfo'].get('taobaoOrders',[]))
        taobaoOrders.createTime = pd.to_datetime(taobaoOrders.createTime)
        last_modify_time = taobaoOrders.createTime.sort_values(ascending=False)[0]
        taobaoOrders_3m = taobaoOrders[(last_modify_time - taobaoOrders["createTime"]).dt.days < 90]
        cnt = taobaoOrders_3m[taobaoOrders_3m.tradeStatusName == "成功"].shape[0]
        taobaoOrders_cnt = TF_2_10(int_compare(cnt,'gt', 2))
    else:
        taobaoOrders_cnt = 0
    result["taobaoOrders_cnt"] = taobaoOrders_cnt

    return result


def run(item):
    try:
        execute(item)
    except Exception as e:
        print(e)
        # 存储处理错误的人到本地文件
        mobile = str(item[1])
        with open("failed.txt", "a") as f:
            f.write(mobile + "\n")

def main():

    pool = ProcessPool(4)
    # 对于每一个人的迭代
    for item in df.itertuples():
        pool.spawn(run,item)





if __name__ == "__main__":
    main()