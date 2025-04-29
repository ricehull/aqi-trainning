import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 创建输出目录
eda_output_dir = 'eda_output'
os.makedirs(eda_output_dir, exist_ok=True)

# 遍历所有站点文件
gsod_files = glob.glob('noaa_data/station_*.csv')

for file_path in gsod_files:
    file_name = os.path.basename(file_path)
    print(f'\nAnalyze: {file_name}')
    df = pd.read_csv(file_path)

    # 输出基本信息
    # print(df.info())
    # print(df.isnull().sum())
    # print(df.describe())

    # 绘制直方图并保存
    # hist_path = os.path.join(eda_output_dir, f'{file_name}_hist.png')
    # df.hist(bins=30, figsize=(15, 10))
    # plt.tight_layout()
    # plt.savefig(hist_path)
    # plt.close()

    # 将所有数值型特征的箱线图按一行5个排列在一张图片上
    num_cols = df.select_dtypes(include='number').columns
    # if len(num_cols) > 0:
    #     n_cols = 5
    #     n_rows = (len(num_cols) + n_cols - 1) // n_cols
    #     fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 4 * n_rows))
    #     axes = axes.flatten() if len(num_cols) > 1 else [axes]
    #     for i, col in enumerate(num_cols):
    #         sns.boxplot(x=df[col], orient='h', ax=axes[i])
    #         axes[i].set_title(f'Boxplot of {col}')
    #     # 隐藏多余的子图
    #     for j in range(len(num_cols), n_rows * n_cols):
    #         fig.delaxes(axes[j])
    #     plt.tight_layout()
    #     all_boxplot_path = os.path.join(eda_output_dir, f'{file_name}_all_boxplot.png')
    #     plt.savefig(all_boxplot_path)
    #     plt.close()

    # 将所有数值型特征的散点图按一行5个排列在一张图片上
    if len(num_cols) > 0:
        n_cols = 5
        n_rows = (len(num_cols) + n_cols - 1) // n_cols
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 4 * n_rows))
        axes = axes.flatten() if len(num_cols) > 1 else [axes]
        for i, col in enumerate(num_cols):
            axes[i].scatter(df.index, df[col], s=10, alpha=0.7)
            axes[i].set_title(f'Scatter of {col}')
            axes[i].set_xlabel('Index')
            axes[i].set_ylabel(col)
        # 隐藏多余的子图
        for j in range(len(num_cols), n_rows * n_cols):
            fig.delaxes(axes[j])
        plt.tight_layout()
        all_scatter_path = os.path.join(eda_output_dir, f'{file_name}_all_scatter.png')
        plt.savefig(all_scatter_path)
        plt.close()