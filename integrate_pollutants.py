import os
import pandas as pd
import logging

class PollutantDataIntegrator:
    def __init__(self):
        self.logger = logging.getLogger('PollutantDataIntegrator')
        
    def read_daily_data(self, station_name, pollutant, year_start, year_end):
        """读取日均值数据(PM2.5和PM10)"""
        filename = f"{station_name}_{pollutant}_{year_start}_{year_end}.csv"
        filepath = os.path.join('openaq_data', filename)
        
        if not os.path.exists(filepath):
            self.logger.warning(f"文件不存在: {filepath}")
            return pd.DataFrame()
            
        try:
            df = pd.read_csv(filepath)
            if df.empty:
                return df
                
            # 确保datetime列存在且格式正确
            if 'day' in df.columns:
                df['datetime'] = pd.to_datetime(df['day'])
            
            # 从average列获取污染物数值
            if 'average' in df.columns:
                df[pollutant] = df['average']
                
            return df[['datetime', pollutant]]
            
        except Exception as e:
            self.logger.error(f"读取文件出错 {filepath}: {str(e)}")
            return pd.DataFrame()
            
    def read_hourly_data(self, station_name, pollutant, year_start, year_end):
        """读取小时级别数据(O3, CO, SO2, NO2)"""
        filename = f"{station_name}_{pollutant}_hourly_{year_start}_{year_end}.csv"
        filepath = os.path.join('openaq_data', filename)
        
        if not os.path.exists(filepath):
            self.logger.warning(f"文件不存在: {filepath}")
            return pd.DataFrame()
            
        try:
            df = pd.read_csv(filepath)
            if df.empty:
                return df
                
            # 确保datetime列存在且格式正确
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
            
            # 从value列获取污染物数值
            if 'value' in df.columns:
                df[pollutant] = df['value']
                
            return df[['datetime', pollutant]]
            
        except Exception as e:
            self.logger.error(f"读取文件出错 {filepath}: {str(e)}")
            return pd.DataFrame()
    
    def process_o3_data(self, df):
        """处理O3数据：计算8小时滚动平均值的每日最大值"""
        if df.empty:
            return df
            
        try:
            # 确保数据按时间排序
            df = df.sort_values('datetime')
            
            # 计算8小时滚动平均值
            df['o3_8hr'] = df['o3'].rolling(window=8, min_periods=6).mean()
            
            # 提取日期部分用于分组
            df['date'] = df['datetime'].dt.date
            
            # 按日期分组并获取每日最大值
            daily_max = df.groupby('date')['o3_8hr'].max().reset_index()
            daily_max['datetime'] = pd.to_datetime(daily_max['date'])
            
            return daily_max[['datetime', 'o3_8hr']].rename(columns={'o3_8hr': 'o3'})
            
        except Exception as e:
            self.logger.error(f"处理O3数据时出错: {str(e)}")
            return pd.DataFrame()
    
    def process_hourly_data(self, df, pollutant):
        """处理CO, SO2, NO2数据：取每日最后一个小时的值"""
        if df.empty:
            return df
            
        try:
            # 确保数据按时间排序
            df = df.sort_values('datetime')
            
            # 提取日期部分用于分组
            df['date'] = df['datetime'].dt.date
            
            # 按日期分组并获取每日最后一个值
            daily_last = df.groupby('date').last().reset_index()
            daily_last['datetime'] = pd.to_datetime(daily_last['date'])
            
            return daily_last[['datetime', pollutant]]
            
        except Exception as e:
            self.logger.error(f"处理{pollutant}数据时出错: {str(e)}")
            return pd.DataFrame()
    
    def integrate_station_data(self, station_name, year_start, year_end):
        """整合单个站点的所有污染物数据"""
        self.logger.info(f"开始处理站点: {station_name}")
        
        # 读取每日数据 (PM2.5和PM10)
        pm25_df = self.read_daily_data(station_name, 'pm25', year_start, year_end)
        pm10_df = self.read_daily_data(station_name, 'pm10', year_start, year_end)
        
        # 读取并处理小时数据
        # O3：8小时滚动平均的每日最大值
        o3_df = self.read_hourly_data(station_name, 'o3', year_start, year_end)
        if not o3_df.empty:
            o3_df = self.process_o3_data(o3_df)
        
        # CO, SO2, NO2：每日最后一小时的值
        co_df = self.read_hourly_data(station_name, 'co', year_start, year_end)
        if not co_df.empty:
            co_df = self.process_hourly_data(co_df, 'co')
            
        so2_df = self.read_hourly_data(station_name, 'so2', year_start, year_end)
        if not so2_df.empty:
            so2_df = self.process_hourly_data(so2_df, 'so2')
            
        no2_df = self.read_hourly_data(station_name, 'no2', year_start, year_end)
        if not no2_df.empty:
            no2_df = self.process_hourly_data(no2_df, 'no2')
        
        # 合并所有数据
        dfs = [pm25_df, pm10_df, o3_df, co_df, so2_df, no2_df]
        dfs = [df for df in dfs if not df.empty]
        
        if not dfs:
            self.logger.warning(f"站点 {station_name} 没有有效数据")
            return pd.DataFrame()
        
        # 以第一个非空数据框为基础，依次合并其他数据
        result = dfs[0]
        for df in dfs[1:]:
            result = pd.merge(result, df, on='datetime', how='outer')
        
        # 确保最终数据按时间排序
        result = result.sort_values('datetime')
        
        # 确保列的顺序符合要求：datetime, pm25, pm10, o3, co, so2, no2
        columns = ['datetime', 'pm25', 'pm10', 'o3', 'co', 'so2', 'no2']
        for col in columns[1:]:
            if col not in result.columns:
                result[col] = None
                
        return result[columns]
    
    def save_integrated_data(self, df, station_name, year_start, year_end):
        """保存整合后的数据"""
        if df.empty:
            self.logger.warning(f"没有数据可保存：站点={station_name}")
            return
            
        # 创建输出目录
        os.makedirs('openaq_integrated_data', exist_ok=True)
        
        # 构建文件名
        filename = f"{station_name}_integrated_{year_start}_{year_end}.csv"
        filepath = os.path.join('openaq_integrated_data', filename)
        
        # 保存数据
        df.to_csv(filepath, index=False)
        self.logger.info(f"数据已保存到: {filepath}")

def main():
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True  # 强制重新配置日志
    )
    logger = logging.getLogger(__name__)
    
    # 初始化整合器
    integrator = PollutantDataIntegrator()
    
    # 定义站点列表
    stations = [
        'bakersfield', 'fresno', 'visalia', 'san-jose',
        'los-angeles', 'phoenix', 'fairbanks', 'lahore-pk'
    ]
    
    # 定义年份范围
    year_start = 2024
    year_end = 2024
    
    # 处理每个站点的数据
    for station_name in stations:
        try:
            # 整合站点数据
            integrated_data = integrator.integrate_station_data(
                station_name,
                year_start,
                year_end
            )
            
            if not integrated_data.empty:
                # 保存整合后的数据
                integrator.save_integrated_data(
                    integrated_data,
                    station_name,
                    year_start,
                    year_end
                )
                
        except Exception as e:
            logger.error(f"处理站点 {station_name} 时出错: {str(e)}", exc_info=True)
            continue

if __name__ == "__main__":
    main()