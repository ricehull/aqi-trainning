import os
import pandas as pd

def merge_all_sites(input_dir='gsod_aqi_merge_data', output_dir='all_gsod_aqi_merged_data', output_file='all_gsod_aqi_merged.csv'):
    os.makedirs(output_dir, exist_ok=True)
    dfs = []
    for fname in os.listdir(input_dir):
        if fname.endswith('.csv'):
            df = pd.read_csv(os.path.join(input_dir, fname))
            site = fname.split('_')[0]
            df['SITE'] = site 
            # 将SITE列移到首列
            cols = df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('SITE')))
            df = df[cols]
            dfs.append(df)
    if dfs:
        all_df = pd.concat(dfs, ignore_index=True)
        out_path = os.path.join(output_dir, output_file)
        all_df.to_csv(out_path, index=False)
        print(f'所有站点数据已合并到: {out_path}')
    else:
        print('未找到可合并的站点数据文件')

def clean_missing_aqi(input_path, output_path=None):
    """
    去除没有AQI数值的行，保存到output_path（默认覆盖原文件），并打印被去除的行。
    """
    df = pd.read_csv(input_path)
    missing_aqi_rows = df[df['AQI'].isna()]
    if not missing_aqi_rows.empty:
        print('被去除的无AQI数值的行:')
        print(missing_aqi_rows)
    df_clean = df.dropna(subset=['AQI'])
    if output_path is None:
        output_path = input_path
    df_clean.to_csv(output_path, index=False)
    print(f'已去除无AQI数值的行: {output_path}')

if __name__ == '__main__':
    merge_all_sites()
    clean_missing_aqi('all_gsod_aqi_merged_data/all_gsod_aqi_merged.csv')