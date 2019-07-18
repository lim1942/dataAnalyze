import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)
# 设置精度
pd.set_option('precision', 5)

from tools.binning import *

def auto_bin(df,label,cols,xlsxName,sheet_name="Sheet1"):
    res_df = pd.DataFrame()
    na_df = pd.DataFrame([{'index': '----', 'name': '------------', 'cate': '----',
                       'amount': '----', 'percent': '----', 'bad_amount': '----',
                       'good_amount': '----', 'bad_rate': '----', 'woe': '----', 'iv': '----', 'IV': '----'}])
    for idx ,col in enumerate(cols):
        df = df.dropna(subset=[col])
        try:
            res = binning_num(df, label, col, 10).reset_index()
        except:
            res = binning_cate(df,label,col).reset_index()
        print(f'=============={idx+1}/{len(cols)}==============')
        print(res)
        res_df = res_df.append(res, ignore_index=True)
        res_df = res_df.append(na_df, ignore_index=True, sort=False)
    if not xlsxName.endswith('xlsx'):
        xlsxName = xlsxName +'.xlsx'
    res_df.to_excel(xlsxName,index=False,sheet_name=sheet_name)