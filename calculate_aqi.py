import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from aqi_breakpoints import breakpoints

class AQICalculator:
    def __init__(self):
        # 设置日志
        logging.basicConfig(level=logging.INFO,
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('AQICalculator')
        
        # AQI breakpoints
        self.breakpoints = breakpoints
        
    def calculate_single_aqi(self, concentration, pollutant):
        """计算单个污染物的AQI值"""
        if concentration is None or np.isnan(concentration):
            return np.nan
            
        # 获取污染物的断点
        breakpoints = self.breakpoints.get(pollutant)
        if not breakpoints:
            self.logger.error(f"未找到污染物 {pollutant} 的断点数据")
            return np.nan
            
        # 查找浓度所在的区间
        for bp_low, bp_high, aqi_low, aqi_high in breakpoints:
            if bp_low <= concentration <= bp_high:
                # 线性插值计算AQI
                aqi = ((aqi_high - aqi_low) / (bp_high - bp_low) * 
                      (concentration - bp_low) + aqi_low)
                return round(aqi)
                
        # 如果浓度超出范围，返回最高AQI值
        if concentration > breakpoints[-1][1]:
            return 500
        return np.nan
        
    def calculate_overall_aqi(self, row):
        """计算总体AQI值和主要污染物"""
        aqi_values = {}
        
        # 计算每个污染物的AQI
        for pollutant in self.breakpoints.keys():
            if pollutant in row and not pd.isna(row[pollutant]):
                aqi = self.calculate_single_aqi(row[pollutant], pollutant)
                if not np.isnan(aqi):
                    aqi_values[pollutant] = aqi
                    
        if not aqi_values:
            return pd.Series({'aqi': np.nan, 'main_pollutant': None})
            
        # 找出最大AQI值和对应的污染物
        max_aqi = max(aqi_values.values())
        main_pollutants = [p for p, v in aqi_values.items() if v == max_aqi]
        
        return pd.Series({
            'aqi': max_aqi,
            'main_pollutant': ','.join(main_pollutants)
        })
        
    def process_station_data(self, station_name, year_start, year_end):
        """处理站点数据并计算AQI"""
        try:
            # 读取整合后的数据文件
            filename = f"{station_name}_integrated_{year_start}_{year_end}.csv"
            filepath = os.path.join('integrated_data', filename)
            
            if not os.path.exists(filepath):
                self.logger.error(f"文件不存在: {filepath}")
                return pd.DataFrame()
                
            df = pd.read_csv(filepath)
            if df.empty:
                self.logger.warning(f"文件为空: {filepath}")
                return df
                
            # 确保datetime列格式正确
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # 计算每个时间点的AQI
            aqi_results = df.apply(self.calculate_overall_aqi, axis=1)
            
            # 合并结果
            df = pd.concat([df, aqi_results], axis=1)
            
            # 添加AQI等级
            df['aqi_level'] = df['aqi'].apply(self.get_aqi_level)
            
            return df
            
        except Exception as e:
            self.logger.error(f"处理站点 {station_name} 数据时出错: {str(e)}")
            return pd.DataFrame()
            
    def get_aqi_level(self, aqi):
        """根据AQI值确定空气质量等级"""
        if pd.isna(aqi):
            return None
            
        if aqi <= 50:
            return 'Good'
        elif aqi <= 100:
            return 'Moderate'
        elif aqi <= 150:
            return 'Unhealthy for Sensitive Groups'
        elif aqi <= 200:
            return 'Unhealthy'
        elif aqi <= 300:
            return 'Very Unhealthy'
        else:
            return 'Hazardous'
            
    def save_aqi_data(self, df, station_name, year_start, year_end):
        """保存AQI计算结果"""
        if df.empty:
            self.logger.warning(f"没有数据可保存：站点={station_name}")
            return
            
        # 创建输出目录
        os.makedirs('aqi_data', exist_ok=True)
        
        # 构建文件名
        filename = f"{station_name}_aqi_{year_start}_{year_end}.csv"
        filepath = os.path.join('aqi_data', filename)
        
        # 保存数据
        df.to_csv(filepath, index=False)
        self.logger.info(f"AQI数据已保存到: {filepath}")

def calculate_pollutant_aqi(concentration, pollutant, breakpoints):
    if concentration is None or pd.isna(concentration):
        return None
    for bp_low, bp_high, aqi_low, aqi_high in breakpoints:
        if bp_low <= concentration <= bp_high:
            aqi = ((aqi_high - aqi_low) / (bp_high - bp_low) * (concentration - bp_low) + aqi_low)
            return round(aqi)
    if concentration > breakpoints[-1][1]:
        return 500
    return None

def process_station_file(input_path, output_path, breakpoints):
    df = pd.read_csv(input_path)
    if df.empty:
        return
    df['datetime'] = pd.to_datetime(df['datetime'])
    pollutants = ['pm25', 'pm10', 'o3', 'co', 'so2', 'no2']
    # 计算每种污染物的AQI
    for p in pollutants:
        df[f'{p}_aqi'] = df[p].apply(lambda x: calculate_pollutant_aqi(x, p, breakpoints[p]))
    # 计算overall AQI
    df['overall_aqi'] = df[[f'{p}_aqi' for p in pollutants]].max(axis=1)
    # 输出列顺序
    out_cols = ['datetime']
    for p in pollutants:
        out_cols.append(p)
        out_cols.append(f'{p}_aqi')
    out_cols.append('overall_aqi')
    df[out_cols].to_csv(output_path, index=False)

def batch_process_all_stations():
    # breakpoints 已通过 import 获得
    stations = [
        'bakersfield', 'fresno', 'visalia', 'san-jose',
        'los-angeles', 'phoenix', 'fairbanks', 'lahore-pk'
    ]
    year_start = 2024
    year_end = 2024
    os.makedirs('aqi_data', exist_ok=True)
    for station in stations:
        input_path = f'integrated_data/{station}_integrated_{year_start}_{year_end}.csv'
        output_path = f'aqi_data/{station}_aqi_{year_start}_{year_end}.csv'
        process_station_file(input_path, output_path, breakpoints)

if __name__ == "__main__":
    batch_process_all_stations()