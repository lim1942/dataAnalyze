import matplotlib.pyplot as plt
import seaborn as sns


# dataframe 相关性的热力图
def heatmap(df,method='spearman'):
    sns.set_context(font_scale=1.5)
    sns.heatmap(df.corr(method=method),vmin=-1,vmax=1,cmap=sns.color_palette("RdBu",n_colors=128))
    plt.show()


#折线图
def pointplot(sl,text=False):
    sns.pointplot(sl.index, sl.values,)
    if text:
        for a, b in zip(range(len(sl.index)),sl.values):
            plt.text(a, b, round(b,2), ha='center', va='bottom', fontsize=10)
    plt.show()


# 柱状图
def barplot(sl,text=False):
    sns.barplot(sl.index, sl.values,)
    if text:
        for a, b in zip(range(len(sl.index)),sl.values):
            plt.text(a, b, round(b,2), ha='center', va='bottom', fontsize=10)
    plt.show()