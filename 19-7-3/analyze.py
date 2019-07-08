import numpy as np
import pandas as pd
import scipy.stats as ss
import matplotlib.pyplot as plt
import seaborn as sns




pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)

import sys
sys.path.append("/Users/apple/Documents/project/analyze/tools")
from binning import binning_sparse_col,binning_cate,binning_num,binning_aeq



df = pd.read_csv("dataset/old_result.csv")
df = df.drop(columns=["mobile"])
sns.set_context(font_scale=1.5)
sns.heatmap(df.corr(method='spearman'),vmin=-1,vmax=1,cmap=sns.color_palette("RdBu",n_colors=128))
plt.show()


for i in ["mobile_rate","contacts_1m","contacts_mobile_1m","contacts_6m","contacts_mobile_6m"]:
    print(binning_num(df, "repay", i, 10))
    print()
