import pandas as pd

"""
特征预处理，删除空值太多的特征，同值太多的特征。空值太多的数据行，少量空值的填充
"""

def missing_delete_var(df, threshold=None):
    """
    删除某个缺失率大于阈值的列
    df:数据集
    threshold:缺失率删除的阈值

    return :删除缺失后的数据集
    """
    df2 = df.copy()
    missing_df = missing_cal(df)
    missing_col_num = missing_df[missing_df.missing_pct >= threshold].shape[0]
    missing_col = list(missing_df[missing_df.missing_pct >= threshold].col)
    # 删除列
    df2 = df2.drop(missing_col, axis=1)
    return df2


def missing_delete_user(df, threshold=None):
    """
    每行数据缺失个数大于阈值就删除该行
    df:数据集
    threshold:缺失个数删除的阈值

    return :删除缺失后的数据集
    """
    df2 = df.copy()
    missing_series = df.isnull().sum(axis=1)
    missing_list = list(missing_series)
    missing_index_list = []
    for i, j in enumerate(missing_list):
        if j >= threshold:
            missing_index_list.append(i)
    # 布尔索引删除行
    df2 = df2[~(df2.index.isin(missing_index_list))]
    return df2


def const_delete(df, col_list, threshold=None):
    """
    df:数据集
    col_list:变量list集合
    threshold:同值化处理的阈值
    同值大于某个阈值就删除这个列属性
    return :处理后的数据集
    """
    df2 = df.copy()
    const_col = []
    for col in col_list:
        const_pct = df2[col].value_counts().iloc[0] / df2[df2[col].notnull()].shape[0]
        if const_pct >= threshold:
            const_col.append(col)
    df2 = df2.drop(const_col, axis=1)
    return df2


def missing_cal(df):
    """
    df :数据集

    return：每个变量/列的缺失率
    """
    missing_series = df.isnull().sum() / df.shape[0]
    missing_df = pd.DataFrame(missing_series).reset_index()
    missing_df = missing_df.rename(columns={'index': 'col',
                                            0: 'missing_pct'})
    missing_df = missing_df.sort_values('missing_pct', ascending=False).reset_index(drop=True)
    return missing_df


def data_processing(df, target):
    """
    df:包含了label和特征的宽表

    return:
    df :清洗后的数据集
    """
    # 特征缺失处理
    df = missing_delete_var(df, threshold=0.8)
    # 样本缺失处理
    df = missing_delete_user(df, threshold=int(df.shape[1] * 0.8))
    col_list = [x for x in df.columns if x != target]
    # 同值化处理
    df = const_delete(df, col_list, threshold=0.9)
    desc = df.describe().T
    # 剔除方差为0的特征
    std_0_col = list(desc[desc['std'] == 0].index)
    if len(std_0_col) > 0:
        df = df.drop(std_0_col, axis=1)
    # 重新添加0+的索引，drop是否删除原索引
    df.reset_index(drop=True, inplace=True)

    # 缺失值计算和填充
    miss_df = missing_cal(df)
    # 按照类型选择变量/列
    cate_col = list(df.select_dtypes(include=['object']).columns)
    num_col = [x for x in list(df.select_dtypes(include=['int64', 'float64']).columns) if x != 'label']

    # 分类型特征填充
    cate_miss_col1 = [x for x in list(miss_df[miss_df.missing_pct > 0.05]['col']) if x in cate_col]
    cate_miss_col2 = [x for x in list(miss_df[miss_df.missing_pct <= 0.05]['col']) if x in cate_col]
    num_miss_col1 = [x for x in list(miss_df[miss_df.missing_pct > 0.05]['col']) if x in num_col]
    num_miss_col2 = [x for x in list(miss_df[miss_df.missing_pct <= 0.05]['col']) if x in num_col]
    for col in cate_miss_col1:
        df[col] = df[col].fillna('未知')
    for col in cate_miss_col2:
        df[col] = df[col].fillna(df[col].mode()[0])
    for col in num_miss_col1:
        df[col] = df[col].fillna(-999)
    for col in num_miss_col2:
        df[col] = df[col].fillna(df[col].median())

    return df, miss_df