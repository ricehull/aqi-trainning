import os
import pandas as pd

class PostEDAGSODDataProcessor:
    """
    处理GSOD数据在EDA过程中发现的问题。
    """
    def __init__(self, output_dir='post_eda_noaa_data'):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.unwanted_columns = [
            'LATITUDE', 'ELEVATION', 'LONGITUDE', 'TEMP_ATTRIBUTES', 'DEWP_ATTRIBUTES',
            'SLP', 'SLP_ATTRIBUTES', 'STP_ATTRIBUTES', 'VISIB_ATTRIBUTES', 'WDSP_ATTRIBUTES',
            'GUST', 'MAX_ATTRIBUTES', 'MIN_ATTRIBUTES', 'PRCP_ATTRIBUTES',
            'SNDP', 'FRSHTT'
        ]

    def drop_unwanted_columns(self, input_path):
        """
        去除不需要的列，并保存到新目录，文件名前加前缀post_eda_
        """
        df = pd.read_csv(input_path)
        df = df.drop(columns=[col for col in self.unwanted_columns if col in df.columns], errors='ignore')
        file_name = os.path.basename(input_path)
        output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(output_path, index=False)
        return output_path

    def fix_temp_outliers(self, input_path):
        """
        处理TEMP列中大于200的异常值：
        - 用前后最近的正常值（<=200）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'TEMP' not in df.columns:
            print(f"{input_path} 无 TEMP 列，跳过")
            return None
        temp = df['TEMP'].values
        to_drop = []
        for i, val in enumerate(temp):
            if val > 200:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and temp[prev_idx] > 200:
                    prev_idx -= 1
                prev_val = temp[prev_idx] if prev_idx >= 0 and temp[prev_idx] <= 200 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(temp) and temp[next_idx] > 200:
                    next_idx += 1
                next_val = temp[next_idx] if next_idx < len(temp) and temp[next_idx] <= 200 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'TEMP'] = fill_val
                    print(f"填充: {input_path} 第{i}行 TEMP={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'TEMP'] = prev_val
                    print(f"填充: {input_path} 第{i}行 TEMP={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'TEMP'] = next_val
                    print(f"填充: {input_path} 第{i}行 TEMP={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 TEMP={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_dewp_outliers(self, input_path):
        """
        处理DEWP列中大于200的异常值：
        - 用前后最近的正常值（<=200）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'DEWP' not in df.columns:
            print(f"{input_path} 无 DEWP 列，跳过")
            return None
        dewp = df['DEWP'].values
        to_drop = []
        for i, val in enumerate(dewp):
            if val > 200:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and dewp[prev_idx] > 200:
                    prev_idx -= 1
                prev_val = dewp[prev_idx] if prev_idx >= 0 and dewp[prev_idx] <= 200 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(dewp) and dewp[next_idx] > 200:
                    next_idx += 1
                next_val = dewp[next_idx] if next_idx < len(dewp) and dewp[next_idx] <= 200 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'DEWP'] = fill_val
                    print(f"填充: {input_path} 第{i}行 DEWP={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'DEWP'] = prev_val
                    print(f"填充: {input_path} 第{i}行 DEWP={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'DEWP'] = next_val
                    print(f"填充: {input_path} 第{i}行 DEWP={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 DEWP={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_stp_outliers(self, input_path):
        """
        处理STP列中等于9999.9的异常值：
        - 用前后最近的正常值（!=9999.9）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'STP' not in df.columns:
            print(f"{input_path} 无 STP 列，跳过")
            return None
        stp = df['STP'].values
        to_drop = []
        for i, val in enumerate(stp):
            if val == 9999.9:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and stp[prev_idx] == 9999.9:
                    prev_idx -= 1
                prev_val = stp[prev_idx] if prev_idx >= 0 and stp[prev_idx] != 9999.9 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(stp) and stp[next_idx] == 9999.9:
                    next_idx += 1
                next_val = stp[next_idx] if next_idx < len(stp) and stp[next_idx] != 9999.9 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'STP'] = fill_val
                    print(f"填充: {input_path} 第{i}行 STP={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'STP'] = prev_val
                    print(f"填充: {input_path} 第{i}行 STP={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'STP'] = next_val
                    print(f"填充: {input_path} 第{i}行 STP={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 STP={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_visib_outliers(self, input_path):
        """
        处理VISIB列中等于999.9的异常值：
        - 用前后最近的正常值（!=999.9）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'VISIB' not in df.columns:
            print(f"{input_path} 无 VISIB 列，跳过")
            return None
        visib = df['VISIB'].values
        to_drop = []
        for i, val in enumerate(visib):
            if val == 999.9:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and visib[prev_idx] == 999.9:
                    prev_idx -= 1
                prev_val = visib[prev_idx] if prev_idx >= 0 and visib[prev_idx] != 999.9 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(visib) and visib[next_idx] == 999.9:
                    next_idx += 1
                next_val = visib[next_idx] if next_idx < len(visib) and visib[next_idx] != 999.9 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'VISIB'] = fill_val
                    print(f"填充: {input_path} 第{i}行 VISIB={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'VISIB'] = prev_val
                    print(f"填充: {input_path} 第{i}行 VISIB={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'VISIB'] = next_val
                    print(f"填充: {input_path} 第{i}行 VISIB={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 VISIB={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_wdsp_outliers(self, input_path):
        """
        处理WDSP列中等于999.9的异常值：
        - 用前后最近的正常值（!=999.9）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'WDSP' not in df.columns:
            print(f"{input_path} 无 WDSP 列，跳过")
            return None
        wdsp = df['WDSP'].values
        to_drop = []
        for i, val in enumerate(wdsp):
            if val == 999.9:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and wdsp[prev_idx] == 999.9:
                    prev_idx -= 1
                prev_val = wdsp[prev_idx] if prev_idx >= 0 and wdsp[prev_idx] != 999.9 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(wdsp) and wdsp[next_idx] == 999.9:
                    next_idx += 1
                next_val = wdsp[next_idx] if next_idx < len(wdsp) and wdsp[next_idx] != 999.9 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'WDSP'] = fill_val
                    print(f"填充: {input_path} 第{i}行 WDSP={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'WDSP'] = prev_val
                    print(f"填充: {input_path} 第{i}行 WDSP={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'WDSP'] = next_val
                    print(f"填充: {input_path} 第{i}行 WDSP={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 WDSP={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_mxspd_outliers(self, input_path):
        """
        处理MXSPD列中等于999.9的异常值：
        - 用前后最近的正常值（!=999.9）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'MXSPD' not in df.columns:
            print(f"{input_path} 无 MXSPD 列，跳过")
            return None
        mxspd = df['MXSPD'].values
        to_drop = []
        for i, val in enumerate(mxspd):
            if val == 999.9:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and mxspd[prev_idx] == 999.9:
                    prev_idx -= 1
                prev_val = mxspd[prev_idx] if prev_idx >= 0 and mxspd[prev_idx] != 999.9 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(mxspd) and mxspd[next_idx] == 999.9:
                    next_idx += 1
                next_val = mxspd[next_idx] if next_idx < len(mxspd) and mxspd[next_idx] != 999.9 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'MXSPD'] = fill_val
                    print(f"填充: {input_path} 第{i}行 MXSPD={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'MXSPD'] = prev_val
                    print(f"填充: {input_path} 第{i}行 MXSPD={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'MXSPD'] = next_val
                    print(f"填充: {input_path} 第{i}行 MXSPD={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 MXSPD={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_max_outliers(self, input_path):
        """
        处理MAX列中等于9999.9的异常值：
        - 用前后最近的正常值（!=9999.9）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'MAX' not in df.columns:
            print(f"{input_path} 无 MAX 列，跳过")
            return None
        max_col = df['MAX'].values
        to_drop = []
        for i, val in enumerate(max_col):
            if val == 9999.9:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and max_col[prev_idx] == 9999.9:
                    prev_idx -= 1
                prev_val = max_col[prev_idx] if prev_idx >= 0 and max_col[prev_idx] != 9999.9 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(max_col) and max_col[next_idx] == 9999.9:
                    next_idx += 1
                next_val = max_col[next_idx] if next_idx < len(max_col) and max_col[next_idx] != 9999.9 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'MAX'] = fill_val
                    print(f"填充: {input_path} 第{i}行 MAX={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'MAX'] = prev_val
                    print(f"填充: {input_path} 第{i}行 MAX={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'MAX'] = next_val
                    print(f"填充: {input_path} 第{i}行 MAX={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 MAX={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_min_outliers(self, input_path):
        """
        处理MIN列中等于9999.9的异常值：
        - 用前后最近的正常值（!=9999.9）均值填充
        - 若只有一侧有正常值，则用那一侧填充
        - 若都没有，则删除该行
        - 打印每次填充或删除的日志
        """
        df = pd.read_csv(input_path)
        if 'MIN' not in df.columns:
            print(f"{input_path} 无 MIN 列，跳过")
            return None
        min_col = df['MIN'].values
        to_drop = []
        for i, val in enumerate(min_col):
            if val == 9999.9:
                # 查找前一条正常值
                prev_idx = i - 1
                while prev_idx >= 0 and min_col[prev_idx] == 9999.9:
                    prev_idx -= 1
                prev_val = min_col[prev_idx] if prev_idx >= 0 and min_col[prev_idx] != 9999.9 else None
                # 查找后一条正常值
                next_idx = i + 1
                while next_idx < len(min_col) and min_col[next_idx] == 9999.9:
                    next_idx += 1
                next_val = min_col[next_idx] if next_idx < len(min_col) and min_col[next_idx] != 9999.9 else None
                # 决定填充值或删除
                if prev_val is not None and next_val is not None:
                    fill_val = (prev_val + next_val) / 2
                    df.at[i, 'MIN'] = fill_val
                    print(f"填充: {input_path} 第{i}行 MIN={val} => {fill_val}")
                elif prev_val is not None:
                    df.at[i, 'MIN'] = prev_val
                    print(f"填充: {input_path} 第{i}行 MIN={val} => {prev_val}")
                elif next_val is not None:
                    df.at[i, 'MIN'] = next_val
                    print(f"填充: {input_path} 第{i}行 MIN={val} => {next_val}")
                else:
                    to_drop.append(i)
                    print(f"删除: {input_path} 第{i}行 MIN={val}，无可用前后值")
        if to_drop:
            df = df.drop(index=to_drop).reset_index(drop=True)
        # file_name = os.path.basename(input_path)
        # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
        df.to_csv(input_path, index=False)
        return input_path

    def fix_prcp_outliers(self, input_path):
        """
        处理PRCP列中等于99.99的异常值：
        - 将其值设置为0.00
        - 打印每次填充的日志
        """
        df = pd.read_csv(input_path)
        if 'PRCP' not in df.columns:
            print(f"{input_path} 无 PRCP 列，跳过")
            return None
        prcp = df['PRCP'].values
        changed = False
        for i, val in enumerate(prcp):
            if val == 99.99:
                df.at[i, 'PRCP'] = 0.00
                print(f"填充: {input_path} 第{i}行 PRCP={val} => 0.00")
                changed = True
        if changed:
            # file_name = os.path.basename(input_path)
            # output_path = os.path.join(self.output_dir, f'post_eda_{file_name}')
            df.to_csv(input_path, index=False)
            return input_path
        return input_path

    def add_month_column(self, input_path):
        """
        根据DATE列生成MONTH列（1~12），并保存到原文件。
        支持date为YYYY-MM-DD或YYYYMMDD格式。
        """
        df = pd.read_csv(input_path)
        if 'DATE' not in df.columns:
            print(f"{input_path} 无 DATE 列，跳过")
            return None
        # 处理日期格式
        try:
            df['MONTH'] = pd.to_datetime(df['DATE'], errors='coerce').dt.month
            if df['MONTH'].isnull().any():
                print(f"警告: {input_path} 存在无法解析的日期")
        except Exception as e:
            print(f"{input_path} 解析日期出错: {e}")
            return None
        df.to_csv(input_path, index=False)
        print(f"已添加MONTH列: {input_path}")
        return input_path

def main():
    input_dir = 'noaa_data'
    processor = PostEDAGSODDataProcessor()
    for file_name in os.listdir(input_dir):
        if file_name.endswith('.csv'):
            input_path = os.path.join(input_dir, file_name)
            # 第一步：去除不需要的列
            base_path = processor.drop_unwanted_columns(input_path)
            print(f'处理完成: {base_path}')
            # 后续所有处理都基于base_path
            processor.fix_temp_outliers(base_path)
            processor.fix_dewp_outliers(base_path)
            processor.fix_stp_outliers(base_path)
            processor.fix_visib_outliers(base_path)
            processor.fix_wdsp_outliers(base_path)
            processor.fix_mxspd_outliers(base_path)
            processor.fix_max_outliers(base_path)
            processor.fix_min_outliers(base_path)
            processor.fix_prcp_outliers(base_path)
            processor.add_month_column(base_path)

if __name__ == '__main__':
    main()
