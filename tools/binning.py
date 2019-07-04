import pandas as pd



def binning_cate(df, target, col):
    """
    类别型变量的直接分箱
    df:数据集
    col:输入的特征
    target:好坏标记的字段名

    return:
    target_df :特征的评估结果 分组里的好坏样本数，bad_rate
    """
    total = df[target].count()
    bad = df[target].sum()
    good = total - bad
    group_result = df.groupby(col).apply(group_handle,target,total,bad,good).reset_index()
    group_result["IV"] = group_result["iv"].sum()
    return group_result


def binning_aeq(df, target, col, max_bin=None, right_border=True):
    """
    等宽分箱，从最小值到最大值之间,均分为 N 等份。
    df:数据集
    col:输入的特征
    target:好坏标记的字段名
    cut:总定义划分区间的list
    right_border：设定左开右闭、左闭右开

    return:
    target_df :特征的评估结果,分组里的好坏样本数，bad_rate
    """
    total = df[target].count()
    bad = df[target].sum()
    good = total - bad
    bucket = pd.cut(df[col], max_bin, right=right_border)
    group_result = df.groupby(bucket).apply(group_handle,target,total,bad,good).reset_index()
    group_result["IV"] = group_result["iv"].sum()
    return group_result


def binning_num(df, target, col, max_bin=None, min_binpct=0):
    """
    卡方分箱
    df:数据集
    col:输入的特征
    target:好坏标记的字段名
    max_bin:最大的分箱个数
    min_binpct:区间内样本所占总体的最小比

    return:
    target_df :特征的评估结果,分组里的好坏样本数，bad_rate
    """
    total = df[target].count()
    bad = df[target].sum()
    good = total - bad
    cut = ChiMerge(df, col, target, max_bin=max_bin, min_binpct=min_binpct)
    cut.insert(0, float('-inf'))
    cut.append(float('inf'))
    bucket = pd.cut(df[col], cut)
    group_result = df.groupby(bucket).apply(group_handle,target,total,bad,good).reset_index()
    group_result["IV"] = group_result["iv"].sum()
    return group_result


def binning_sparse_col(df, target, col, max_bin=None, min_binpct=0, sparse_value=-1):
    """
    有特殊值的卡方分箱
    df:数据集
    col:输入的特征
    target:好坏标记的字段名
    max_bin:最大的分箱个数
    min_binpct:区间内样本所占总体的最小比
    sparse_value: 特殊分组的

    return:
    target_df :特征的评估结果,分组里的好坏样本数，bad_rate
    """
    total = df[target].count()
    bad = df[target].sum()
    good = total - bad

    # 拆分数据集
    temp1 = df[df[col] == sparse_value]
    temp2 = df[~(df[col] == sparse_value)]

    # 特殊值单独分箱
    bucket_sparse = pd.cut(temp1[col], [float('-inf'), sparse_value])
    group1_result = temp1.groupby(bucket_sparse).apply(group_handle,target,total,bad,good)

    # 剩余数据卡方分箱
    cut = ChiMerge(temp2, col, target, max_bin=max_bin, min_binpct=min_binpct)
    cut.insert(0, sparse_value)
    cut.append(float('inf'))
    bucket = pd.cut(temp2[col], cut)
    group2_result = temp2.groupby(bucket).apply(group_handle,target,total,bad,good)
    # 返回统计结果
    group_result = pd.concat([group1_result.reset_index(), group2_result.reset_index()], axis=0).reset_index(drop=True)
    group_result["IV"] = group_result["iv"].sum()
    return group1_result


def group_handle(group,target,total,bad,good):
    """
    一.IV值，特征对lable的IV值
    < 0.02预测能力很弱、
    0.02 to 0.1预测能力弱、
    0.1 to 0.3预测能力中、
    0.3 to 0.5预测能力强 、
    >0.5 很强的预测能力
    二.woe值：WOE表示的含义即是"当前分组中响应客户
    占所有响应客户的比例"和"当前分组中没有响应的客
    户占所有没有响应客户的比例"的差异。
    """
    sample_amount = group.shape[0]
    sample_bad_amount = group[target].sum()
    sample_bad_percent = sample_bad_amount/bad
    sample_good_amount = sample_amount - sample_bad_amount
    sample_good_percent= sample_good_amount/good
    woe = pd.np.log(sample_bad_percent/sample_good_percent) if (sample_bad_percent and sample_good_percent) else None
    return pd.Series({
        # 样本数
        "amount":sample_amount,
        # 样本占比
        "percent":sample_amount/total,
        # 好样本数
        "bad_amount":group[target].sum(),
        # 坏样本数
        "good_amount":sample_good_amount,
        # 样本黑样本占比
        "bad_rate":sample_bad_amount/sample_amount,
        # woe
        "woe":woe,
        # iv值
        "iv":(sample_bad_percent-sample_good_percent)*woe if woe else None
    })


# 先用卡方分箱输出变量的分割点
def split_data(df, col, split_num):
    """
    df: 原始数据集
    col:需要分箱的变量
    split_num:分割点的数量
    """
    df2 = df.copy()
    count = df2.shape[0]  # 总样本数
    n = pd.np.floor(count / split_num)  # 按照分割点数目等分后每组的样本数
    split_index = [i * n for i in range(1, split_num)]  # 分割点的索引
    values = sorted(list(df2[col]))  # 对变量的值从小到大进行排序
    split_value = [values[i] for i in split_index]  # 分割点对应的value
    split_value = sorted(list(set(split_value)))  # 分割点的value去重排序
    return split_value


def assign_group(x, split_bin):
    """
    x:变量的value
    split_bin:split_data得出的分割点list
    """
    n = len(split_bin)
    if x <= min(split_bin):
        return min(split_bin)  # 如果x小于分割点的最小值，则x映射为分割点的最小值
    elif x > max(split_bin):  # 如果x大于分割点的最大值，则x映射为分割点的最大值
        return 10e10
    else:
        for i in range(n - 1):
            if split_bin[i] < x <= split_bin[i + 1]:  # 如果x在两个分割点之间，则x映射为分割点较大的值
                return split_bin[i + 1]


def bin_bad_rate(df, col, target, grantRateIndicator=0):
    """
    df:原始数据集
    col:原始变量/变量映射后的字段
    target:目标变量的字段
    grantRateIndicator:是否输出总体的违约率
    """
    total = df.groupby([col])[target].count()
    bad = df.groupby([col])[target].sum()
    total_df = pd.DataFrame({'total': total})
    bad_df = pd.DataFrame({'bad': bad})
    regroup = pd.merge(total_df, bad_df, left_index=True, right_index=True, how='left')
    regroup = regroup.reset_index()
    regroup['bad_rate'] = regroup['bad'] / regroup['total']  # 计算根据col分组后每组的违约率
    dict_bad = dict(zip(regroup[col], regroup['bad_rate']))  # 转为字典形式
    if grantRateIndicator == 0:
        return (dict_bad, regroup)
    total_all = df.shape[0]
    bad_all = df[target].sum()
    all_bad_rate = bad_all / total_all  # 计算总体的违约率
    return (dict_bad, regroup, all_bad_rate)


def cal_chi2(df, all_bad_rate):
    """
    df:bin_bad_rate得出的regroup
    all_bad_rate:bin_bad_rate得出的总体违约率
    """
    df2 = df.copy()
    df2['expected'] = df2['total'] * all_bad_rate  # 计算每组的坏用户期望数量
    combined = zip(df2['expected'], df2['bad'])  # 遍历每组的坏用户期望数量和实际数量
    chi = [(i[0] - i[1]) ** 2 / i[0] for i in combined]  # 计算每组的卡方值
    chi2 = sum(chi)  # 计算总的卡方值
    return chi2


def assign_bin(x, cutoffpoints):
    """
    x:变量的value
    cutoffpoints:分箱的切割点
    """
    bin_num = len(cutoffpoints) + 1  # 箱体个数
    if x <= cutoffpoints[0]:  # 如果x小于最小的cutoff点，则映射为Bin 0
        return 'Bin 0'
    elif x > cutoffpoints[-1]:  # 如果x大于最大的cutoff点，则映射为Bin(bin_num-1)
        return 'Bin {}'.format(bin_num - 1)
    else:
        for i in range(0, bin_num - 1):
            if cutoffpoints[i] < x <= cutoffpoints[i + 1]:  # 如果x在两个cutoff点之间，则x映射为Bin(i+1)
                return 'Bin {}'.format(i + 1)


def ChiMerge(df, col, target, max_bin=5, min_binpct=0):
    col_unique = sorted(list(set(df[col])))  # 变量的唯一值并排序
    n = len(col_unique)  # 变量唯一值得个数
    df2 = df.copy()
    if n > 100:  # 如果变量的唯一值数目超过100，则将通过split_data和assign_group将x映射为split对应的value
        split_col = split_data(df2, col, 100)  # 通过这个目的将变量的唯一值数目人为设定为100
        df2['col_map'] = df2[col].map(lambda x: assign_group(x, split_col))
    else:
        df2['col_map'] = df2[col]  # 变量的唯一值数目没有超过100，则不用做映射
    # 生成dict_bad,regroup,all_bad_rate的元组
    (dict_bad, regroup, all_bad_rate) = bin_bad_rate(df2, 'col_map', target, grantRateIndicator=1)
    col_map_unique = sorted(list(set(df2['col_map'])))  # 对变量映射后的value进行去重排序
    group_interval = [[i] for i in col_map_unique]  # 对col_map_unique中每个值创建list并存储在group_interval中

    while (len(group_interval) > max_bin):  # 当group_interval的长度大于max_bin时，执行while循环
        chi_list = []
        for i in range(len(group_interval) - 1):
            temp_group = group_interval[i] + group_interval[i + 1]  # temp_group 为生成的区间,list形式，例如[1,3]
            chi_df = regroup[regroup['col_map'].isin(temp_group)]
            chi_value = cal_chi2(chi_df, all_bad_rate)  # 计算每一对相邻区间的卡方值
            chi_list.append(chi_value)
        best_combined = chi_list.index(min(chi_list))  # 最小的卡方值的索引
        # 将卡方值最小的一对区间进行合并
        group_interval[best_combined] = group_interval[best_combined] + group_interval[best_combined + 1]
        # 删除合并前的右区间
        group_interval.remove(group_interval[best_combined + 1])
        # 对合并后每个区间进行排序
    group_interval = [sorted(i) for i in group_interval]
    # cutoff点为每个区间的最大值
    cutoffpoints = [max(i) for i in group_interval[:-1]]

    # 检查是否有箱只有好样本或者只有坏样本
    df2['col_map_bin'] = df2['col_map'].apply(lambda x: assign_bin(x, cutoffpoints))  # 将col_map映射为对应的区间Bin
    # 计算每个区间的违约率
    (dict_bad, regroup) = bin_bad_rate(df2, 'col_map_bin', target)
    # 计算最小和最大的违约率
    [min_bad_rate, max_bad_rate] = [min(dict_bad.values()), max(dict_bad.values())]
    # 当最小的违约率等于0，说明区间内只有好样本，当最大的违约率等于1，说明区间内只有坏样本
    while min_bad_rate == 0 or max_bad_rate == 1:
        bad01_index = regroup[regroup['bad_rate'].isin([0, 1])].col_map_bin.tolist()  # 违约率为1或0的区间
        bad01_bin = bad01_index[0]
        if bad01_bin == max(regroup.col_map_bin):
            cutoffpoints = cutoffpoints[:-1]  # 当bad01_bin是最大的区间时，删除最大的cutoff点
        elif bad01_bin == min(regroup.col_map_bin):
            cutoffpoints = cutoffpoints[1:]  # 当bad01_bin是最小的区间时，删除最小的cutoff点
        else:
            bad01_bin_index = list(regroup.col_map_bin).index(bad01_bin)  # 找出bad01_bin的索引
            prev_bin = list(regroup.col_map_bin)[bad01_bin_index - 1]  # bad01_bin前一个区间
            df3 = df2[df2.col_map_bin.isin([prev_bin, bad01_bin])]
            (dict_bad, regroup1) = bin_bad_rate(df3, 'col_map_bin', target)
            chi1 = cal_chi2(regroup1, all_bad_rate)  # 计算前一个区间和bad01_bin的卡方值
            later_bin = list(regroup.col_map_bin)[bad01_bin_index + 1]  # bin01_bin的后一个区间
            df4 = df2[df2.col_map_bin.isin([later_bin, bad01_bin])]
            (dict_bad, regroup2) = bin_bad_rate(df4, 'col_map_bin', target)
            chi2 = cal_chi2(regroup2, all_bad_rate)  # 计算后一个区间和bad01_bin的卡方值
            if chi1 < chi2:  # 当chi1<chi2时,删除前一个区间对应的cutoff点
                cutoffpoints.remove(cutoffpoints[bad01_bin_index - 1])
            else:  # 当chi1>=chi2时,删除bin01对应的cutoff点
                cutoffpoints.remove(cutoffpoints[bad01_bin_index])
        df2['col_map_bin'] = df2['col_map'].apply(lambda x: assign_bin(x, cutoffpoints))
        (dict_bad, regroup) = bin_bad_rate(df2, 'col_map_bin', target)
        # 重新将col_map映射至区间，并计算最小和最大的违约率，直达不再出现违约率为0或1的情况，循环停止
        [min_bad_rate, max_bad_rate] = [min(dict_bad.values()), max(dict_bad.values())]

    # 检查分箱后的最小占比
    if min_binpct > 0:
        group_values = df2['col_map'].apply(lambda x: assign_bin(x, cutoffpoints))
        df2['col_map_bin'] = group_values  # 将col_map映射为对应的区间Bin
        group_df = group_values.value_counts().to_frame()
        group_df['bin_pct'] = group_df['col_map'] / n  # 计算每个区间的占比
        min_pct = group_df.bin_pct.min()  # 得出最小的区间占比
        while min_pct < min_binpct and len(cutoffpoints) > 2:  # 当最小的区间占比小于min_pct且cutoff点的个数大于2，执行循环
            # 下面的逻辑基本与“检验是否有箱体只有好/坏样本”的一致
            min_pct_index = group_df[group_df.bin_pct == min_pct].index.tolist()
            min_pct_bin = min_pct_index[0]
            if min_pct_bin == max(group_df.index):
                cutoffpoints = cutoffpoints[:-1]
            elif min_pct_bin == min(group_df.index):
                cutoffpoints = cutoffpoints[1:]
            else:
                minpct_bin_index = list(group_df.index).index(min_pct_bin)
                prev_pct_bin = list(group_df.index)[minpct_bin_index - 1]
                df5 = df2[df2['col_map_bin'].isin([min_pct_bin, prev_pct_bin])]
                (dict_bad, regroup3) = bin_bad_rate(df5, 'col_map_bin', target)
                chi3 = cal_chi2(regroup3, all_bad_rate)
                later_pct_bin = list(group_df.index)[minpct_bin_index + 1]
                df6 = df2[df2['col_map_bin'].isin([min_pct_bin, later_pct_bin])]
                (dict_bad, regroup4) = bin_bad_rate(df6, 'col_map_bin', target)
                chi4 = cal_chi2(regroup4, all_bad_rate)
                if chi3 < chi4:
                    cutoffpoints.remove(cutoffpoints[minpct_bin_index - 1])
                else:
                    cutoffpoints.remove(cutoffpoints[minpct_bin_index])
    return cutoffpoints
