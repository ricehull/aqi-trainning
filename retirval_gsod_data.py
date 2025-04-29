import os
import boto3
import pandas as pd
import numpy as np
import logging
from io import StringIO
from botocore.client import Config
from botocore.config import Config
from botocore import UNSIGNED

class NOAADataDownloader:
    def __init__(self):
        self.s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        self.noaagsod_bucket = 'noaa-gsod-pds'
        
        # 设置日志
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('NOAADownloader')
        
        # 更新数据质量控制参数，使用更严格的范围
        self.qc_limits = {
            'TEMP': {'min': -40, 'max': 50},      # 温度范围 (-40°C to 50°C)
            'DEWP': {'min': -40, 'max': 35},      # 露点温度范围
            'STP': {'min': 870, 'max': 1085},     # 站点气压范围 (hPa)
            'VISIB': {'min': 0, 'max': 80},       # 能见度范围 (km)
            'WDSP': {'min': 0, 'max': 50},        # 风速范围 (knots)
            'MXSPD': {'min': 0, 'max': 100},      # 最大阵风范围 (knots)
            'MAX': {'min': -40, 'max': 50},       # 最高温度范围
            'MIN': {'min': -40, 'max': 50},       # 最低温度范围
            'PRCP': {'min': 0, 'max': 500}        # 降水量范围 (mm)
        }
    
    def validate_data(self, df):
        """验证数据的有效性"""
        if df.empty:
            self.logger.warning("数据集为空")
            return df
            
        # 检查必需的列
        required_columns = ['DATE', 'TEMP', 'DEWP', 'SLP', 'STP', 'VISIB', 'WDSP']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"缺少必需的列: {missing_columns}")
            
        # 对每个参数进行范围检查
        for param, limits in self.qc_limits.items():
            if param in df.columns:
                # 转换为数值类型，无效值设为NaN
                df[param] = pd.to_numeric(df[param], errors='coerce')
                
                # 检查是否所有值都是NaN
                if df[param].isnull().all():
                    self.logger.warning(f"参数 {param} 的所有值都无效")
                    continue
                    
                # 应用范围检查
                mask = (df[param] >= limits['min']) & (df[param] <= limits['max'])
                invalid_count = (~mask).sum()
                
                if invalid_count > 0:
                    self.logger.warning(f"参数 {param} 中有 {invalid_count} 个值超出范围 "
                                      f"[{limits['min']}, {limits['max']}]")
                    # 将超出范围的值设为NaN
                    df.loc[~mask, param] = np.nan
                    
                # 检查缺失值比例
                missing_ratio = df[param].isnull().mean()
                if missing_ratio > 0.3:  # 如果缺失值超过30%
                    self.logger.warning(f"参数 {param} 的缺失值比例较高: {missing_ratio:.1%}")
                    
        return df
        
    def interpolate_missing_data(self, df, max_gap_days=3):
        """插值处理缺失数据"""
        if df.empty:
            return df
            
        # 设置日期索引
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.set_index('DATE')
        
        # 创建完整的日期序列
        full_range = pd.date_range(start=df.index.min(),
                                 end=df.index.max(),
                                 freq='D')
                                 
        # 重新索引数据框
        df = df.reindex(full_range)
        
        # 对数值列进行插值
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            # 检查缺失值的数量
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                self.logger.info(f"对列 {col} 进行插值处理，缺失值数量: {missing_count}")
                
                # 使用线性插值填充短期缺失
                df[col] = df[col].interpolate(method='linear',
                                            limit=max_gap_days,
                                            limit_direction='both')
                                            
                # 统计剩余的缺失值
                remaining_missing = df[col].isnull().sum()
                if remaining_missing > 0:
                    self.logger.warning(f"列 {col} 在插值后仍有 {remaining_missing} 个缺失值")
                    
        # 重置索引
        df = df.reset_index()
        df = df.rename(columns={'index': 'DATE'})
        
        return df

    def download_station_data(self, station_id, year_start, year_end, output_dir='noaa_data'):
        """下载指定站点的数据"""
        os.makedirs(output_dir, exist_ok=True)
        station_df = pd.DataFrame()
        
        self.logger.info(f'正在下载站点 {station_id} 的数据...')
        
        for year in range(year_start, year_end + 1):
            try:
                key = f'{year}/{station_id}.csv'
                csv_obj = self.s3.get_object(Bucket=self.noaagsod_bucket, Key=key)
                csv_string = csv_obj['Body'].read().decode('utf-8')
                year_df = pd.read_csv(StringIO(csv_string))
                
                # 验证数据 TODO
                # year_df = self.validate_data(year_df)
                
                if not year_df.empty:
                    station_df = pd.concat([station_df, year_df], ignore_index=True)
                    self.logger.info(f'已下载并验证 {year} 年数据')
            except Exception as e:
                self.logger.error(f'下载 {year} 年数据时出错: {str(e)}')
                continue
        
        if len(station_df) > 0:
            # 插值处理缺失数据 TODO
            # station_df = self.interpolate_missing_data(station_df)
            
            # 添加月份列 TODO
            # station_df['MONTH'] = pd.to_datetime(station_df['DATE']).dt.month
            
            # 保存到CSV文件
            output_file = f'{output_dir}/station_{station_id}_{year_start}_{year_end}.csv'
            station_df.to_csv(output_file, index=False)
            self.logger.info(f'数据已保存到: {output_file}')
            
            # 输出数据质量报告 TODO
            # self._generate_quality_report(station_df, station_id)
            
            return output_file
        else:
            self.logger.warning(f'站点 {station_id} 没有找到有效数据')
            return None
            
    def _generate_quality_report(self, df, station_id):
        """生成数据质量报告"""
        self.logger.info(f"\n数据质量报告 - 站点 {station_id}:")
        
        # 计算每个参数的统计信息
        for column in df.select_dtypes(include=[np.number]).columns:
            if column in self.qc_limits:
                stats = df[column].describe()
                missing_ratio = df[column].isnull().mean()
                
                self.logger.info(f"\n{column}:")
                self.logger.info(f"  范围: [{self.qc_limits[column]['min']}, {self.qc_limits[column]['max']}]")
                self.logger.info(f"  实际范围: [{stats['min']:.1f}, {stats['max']:.1f}]")
                self.logger.info(f"  均值: {stats['mean']:.1f}")
                self.logger.info(f"  标准差: {stats['std']:.1f}")
                self.logger.info(f"  缺失值比例: {missing_ratio:.1%}")
                
    def batch_download(self, station_ids, year_start, year_end, output_dir='noaa_data'):
        """批量下载多个站点的数据"""
        output_files = []
        for station_id in station_ids:
            output_file = self.download_station_data(station_id, year_start, year_end, output_dir)
            if output_file:
                output_files.append(output_file)
        return output_files

# 使用示例
# 创建下载器实例
downloader = NOAADataDownloader()

# 示例站点列表 (使用项目中已有的一些站点ID)
# example_stations = [
#     "72384023155",  # bakersfield
#     "72389093193",  # fresno
#     "72389693144",  # visalia
# ]
station_ids = [
    '72384023155',  # bakersfield 
    '72389093193',  # fresno 
    '72389693144',  # visalia
    '72494693232',  # san-jose
    '72287493134',  # los-angeles
    '72278023183',  # phoenix
    '70261026411',  # fairbanks
    '41640099999'   # lahore-pk
]  

# 设置年份范围
year_start = 2024  # 修改为仅2024年
year_end = 2024    # 修改为仅2024年

# 批量下载数据
output_files = downloader.batch_download(station_ids, year_start, year_end)
print("\n下载完成的文件列表:")
for f in output_files:
    print(f)