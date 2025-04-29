import os
import requests
import pandas as pd
from time import sleep
from io import StringIO
from botocore.client import Config
from botocore.config import Config
from botocore import UNSIGNED
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.exceptions import ClientError
from botocore.exceptions import BotoCoreError
from botocore.exceptions import EndpointConnectionError


class OpenAQDataDownloader:
    def __init__(self, api_key=None):
        self.api_base = "https://api.openaq.org/v3"
        self.api_key = api_key
        self.headers = {'X-API-Key': api_key} if api_key else {}
        self.noaa_stations = self._load_noaa_stations()

    def _load_noaa_stations(self):
        """加载NOAA站点信息"""
        stations = {}
        # 添加NOAA站点信息
        stations['72384023155'] = {'name': 'bakersfield', 'lat': 35.433, 'lon': -119.050}
        stations['72389093193'] = {'name': 'fresno', 'lat': 36.778, 'lon': -119.719}
        stations['72389693144'] = {'name': 'visalia', 'lat': 36.317, 'lon': -119.383}
        stations['72494693232'] = {'name': 'san-jose', 'lat': 37.359, 'lon': -121.924}
        stations['72287493134'] = {'name': 'los-angeles', 'lat': 33.938, 'lon': -118.389}
        stations['72278023183'] = {'name': 'phoenix', 'lat': 33.428, 'lon': -112.004}
        stations['70261026411'] = {'name': 'fairbanks', 'lat': 64.804, 'lon': -147.876}
        stations['41640099999'] = {'name': 'lahore-pk', 'lat': 31.521, 'lon': 74.327}
        return stations

    def get_location_ids(self, station_id, parameter, radius_meters=25000):
        """获取指定NOAA站点附近的OpenAQ监测站点ID"""
        if station_id not in self.noaa_stations:
            print(f'警告: 未找到站点 {station_id} 的坐标信息')
            return []

        station = self.noaa_stations[station_id]
        params = {
            'limit': 10,
            'page': 1,
            'offset': 0,
            'parameter': parameter,
            'coordinates': f"{station['lat']},{station['lon']}",
            'radius': radius_meters,
            'has_geo': True
        }

        try:
            response = requests.get(f"{self.api_base}/locations", params=params, headers=self.headers)
            print(f"API URL: {response.url}")
            print(f"Status Code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"API请求失败，状态码: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return []
                
            data = response.json()
            if 'results' in data and data['results']:
                return [loc['id'] for loc in data['results']]
            else:
                print(f"API返回数据中没有results字段或结果为空: {data}")
                return []
        except Exception as e:
            print(f'获取监测站点ID时出错: {str(e)}')
            return []

    def get_sensor_ids(self, location_id, parameter):
        """获取指定location的sensor IDs"""
        try:
            response = requests.get(f"{self.api_base}/locations/{location_id}/sensors", headers=self.headers)
            print(f"获取sensor IDs - Status Code: {response.status_code}")
            
            if response.status_code != 200:
                print(f"获取sensor失败，状态码: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return []
                
            data = response.json()
            if 'results' in data and data['results']:
                # 只返回匹配参数的sensor IDs
                return [sensor['id'] for sensor in data['results'] 
                       if sensor.get('parameter', {}).get('name', '').lower() == parameter.lower()]
            return []
        except Exception as e:
            print(f'获取sensor IDs时出错: {str(e)}')
            return []

    def get_daily_measurements(self, sensor_id, year_start, year_end):
        """获取传感器的每日聚合数据"""
        all_data = []
        
        for year in range(year_start, year_end + 1):
            params = {
                'datetime_from': f'{year}-01-01',
                'datetime_to': f'{year}-12-31',
                'limit': 366,
                'page': 1,
                'offset': 0
            }
            
            try:
                response = requests.get(
                    f"{self.api_base}/sensors/{sensor_id}/measurements/daily",
                    params=params,
                    headers=self.headers
                )
                print(f"获取{year}年数据 - Status Code: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"获取数据失败，状态码: {response.status_code}")
                    print(f"Response: {response.text[:500]}")
                    continue
                    
                data = response.json()
                if 'results' in data and data['results']:
                    all_data.extend(data['results'])
                    print(f'获取到 {len(data["results"])} 条{year}年记录')
            except Exception as e:
                print(f'获取{year}年数据时出错: {str(e)}')
                continue
                
        return all_data

    def process_measurements_data(self, measurements, year_start, year_end):
        """处理测量数据，提取关键信息并格式化"""
        processed_data = []
        for m in measurements:
            try:
                # 从period中提取日期时间信息
                date_from = m['period']['datetimeFrom']['utc']
                date_year = int(date_from[:4])
                
                # 只处理指定年份范围内的数据
                if year_start <= date_year <= year_end:
                    # 提取基本数据
                    processed_row = {
                        'day': date_from[:10],  # 只保留日期部分 YYYY-MM-DD
                        'average': m['value'],
                        'unit': m['parameter']['units'],
                        'parameter': m['parameter']['name'],
                        'coverage': m['coverage']['percentComplete']
                    }
                    processed_data.append(processed_row)
            except (KeyError, TypeError) as e:
                print(f"处理数据时出错: {str(e)}")
                continue
        return processed_data

    def download_station_data(self, station_id, parameter, year_start, year_end, output_dir='openaq_data'):
        """下载指定站点的OpenAQ数据"""
        if not self.api_key:
            print('警告: 未提供API密钥。请从 https://openaq.org 获取API密钥')
            return None

        os.makedirs(output_dir, exist_ok=True)
        station_name = self.noaa_stations[station_id]['name']
        
        print(f'正在处理站点 {station_name} ({station_id})...')
        location_ids = self.get_location_ids(station_id, parameter)
        
        if not location_ids:
            print(f'警告: 站点 {station_name} 附近未找到OpenAQ监测站点')
            return None
        
        print(f'找到 {len(location_ids)} 个OpenAQ监测站点')
        
        # 创建空的列表用于存储处理后的数据
        all_processed_data = []
        
        # 遍历每个location获取sensor IDs和数据
        for location_id in location_ids:
            print(f'正在获取location {location_id}的sensors...')
            sensor_ids = self.get_sensor_ids(location_id, parameter)
            
            if not sensor_ids:
                print(f'警告: location {location_id}没有找到匹配的sensors')
                continue
                
            print(f'找到 {len(sensor_ids)} 个sensors')
            
            # 获取每个sensor的数据
            for sensor_id in sensor_ids:
                print(f'正在获取sensor {sensor_id}的数据...')
                measurements = self.get_daily_measurements(sensor_id, year_start, year_end)
                if measurements:
                    processed_data = self.process_measurements_data(measurements, year_start, year_end)
                    all_processed_data.extend(processed_data)
                sleep(3)  # 避免请求过快导致API限制
        
        if all_processed_data:
            # 转换为DataFrame
            df = pd.DataFrame(all_processed_data)
            
            # 按日期分组并计算平均值
            df = df.groupby('day', as_index=False).agg({
                'average': 'mean',
                'coverage': 'mean',
                'unit': 'first',
                'parameter': 'first'
            })
            
            # 按日期排序
            df = df.sort_values('day')
            
            # 保存到CSV文件
            output_file = f'{output_dir}/{station_name}_{parameter}_{year_start}_{year_end}.csv'
            df.to_csv(output_file, index=False)
            print(f'数据已保存到: {output_file}')
            return output_file
        else:
            print(f'警告: 站点 {station_name} 没有找到任何数据')
            return None

    def get_hourly_measurements(self, sensor_id, year_start, year_end):
        """获取传感器的每小时数据"""
        all_data = []
        
        for year in range(year_start, year_end + 1):
            print(f"正在获取{year}年的小时数据...")
            
            # 分月获取数据以避免数据量过大
            for month in range(1, 13):
                # 设置日期范围
                if month == 12:
                    date_from = f'{year}-{month:02d}-01'
                    date_to = f'{year}-{month:02d}-31'
                else:
                    date_from = f'{year}-{month:02d}-01'
                    date_to = f'{year}-{month+1:02d}-01'
                print(f"获取{year}年{month}月数据: {date_from} 到 {date_to}")
                
                params = {
                    'datetime_from': date_from,
                    'datetime_to': date_to,
                    'limit': 744,  # 最大一个月的小时数
                    'page': 1,
                    'offset': 0
                }
                
                try:
                    response = requests.get(
                        f"{self.api_base}/sensors/{sensor_id}/measurements/hourly",
                        params=params,
                        headers=self.headers
                    )
                    print(f"获取{year}年{month}月数据 - Status Code: {response.status_code}")
                    sleep(3)  # 避免请求过快导致API限制
 
                    if response.status_code != 200:
                        print(f"获取数据失败，状态码: {response.status_code}")
                        print(f"Response: {response.text[:500]}")
                        continue
                        
                    data = response.json()
                    if 'results' in data and data['results']:
                        all_data.extend(data['results'])
                        print(f'获取到 {len(data["results"])} 条{year}年{month}月记录')
                except Exception as e:
                    print(f'获取{year}年{month}月数据时出错: {str(e)}')
                    continue
                    
        return all_data

    def process_hourly_measurements(self, measurements, parameter):
        """处理小时测量数据，提取关键信息并格式化"""
        processed_data = []
        for m in measurements:
            try:
                # 检查是否存在 'datetime' 或 'day' 字段
                if 'datetime' in m:
                    date_time = m['datetime']['utc']
                elif 'period' in m and 'datetimeFrom' in m['period']:
                    date_time = m['period']['datetimeFrom']['utc']
                else:
                    print("警告: 找不到日期时间字段")
                    continue
                
                # 提取基本数据
                processed_row = {
                    'day': date_time[:10],  # 保存日期部分
                    'hour': date_time[11:13],  # 保存小时部分
                    'datetime': date_time,  # 保存完整的datetime
                    'value': m['value'],
                    'unit': m['parameter']['units'],
                    'parameter': m['parameter']['name']
                }
                processed_data.append(processed_row)
            except (KeyError, TypeError) as e:
                print(f"处理数据时出错: {str(e)}, 数据结构: {m}")
                continue
        return processed_data

    def download_station_hourly_data(self, station_id, parameter, year_start, year_end, output_dir='openaq_data'):
        """下载指定站点的OpenAQ小时数据"""
        if not self.api_key:
            print('警告: 未提供API密钥。请从 https://openaq.org 获取API密钥')
            return None

        os.makedirs(output_dir, exist_ok=True)
        station_name = self.noaa_stations[station_id]['name']
        
        print(f'正在处理站点 {station_name} ({station_id})...')
        location_ids = self.get_location_ids(station_id, parameter)
        
        if not location_ids:
            print(f'警告: 站点 {station_name} 附近未找到OpenAQ监测站点')
            return None
        
        print(f'找到 {len(location_ids)} 个OpenAQ监测站点')
        
        # 创建空的列表用于存储处理后的数据
        all_processed_data = []
        
        # 遍历每个location获取sensor IDs和数据
        for location_id in location_ids:
            print(f'正在获取location {location_id}的sensors...')
            sensor_ids = self.get_sensor_ids(location_id, parameter)
            
            if not sensor_ids:
                print(f'警告: location {location_id}没有找到匹配的sensors')
                continue
                
            print(f'找到 {len(sensor_ids)} 个sensors')
            
            # 获取每个sensor的数据
            for sensor_id in sensor_ids:
                print(f'正在获取sensor {sensor_id}的小时数据...')
                measurements = self.get_hourly_measurements(sensor_id, year_start, year_end)
                if measurements:
                    processed_data = self.process_hourly_measurements(measurements, parameter)
                    all_processed_data.extend(processed_data)
        
        if all_processed_data:
            # 转换为DataFrame
            df = pd.DataFrame(all_processed_data)
            
            # 确保datetime列是日期时间类型
            df['datetime'] = pd.to_datetime(df['datetime'])
            
            # 按时间排序
            df = df.sort_values('datetime')
            
            # 如果同一时间有多个传感器的数据，取平均值
            df = df.groupby('datetime').agg({
                'value': 'mean',
                'unit': 'first',
                'parameter': 'first'
            }).reset_index()
            
            # 保存到CSV文件
            output_file = f'{output_dir}/{station_name}_{parameter}_hourly_{year_start}_{year_end}.csv'
            df.to_csv(output_file, index=False)
            print(f'数据已保存到: {output_file}')
            return output_file
        else:
            print(f'警告: 站点 {station_name} 没有找到任何数据')
            return None

    def batch_download(self, station_ids, parameter, year_start, year_end, output_dir='openaq_data'):
        """批量下载多个站点的数据"""
        output_files = []
        
        # 根据参数类型选择下载方法
        if parameter in ['pm25', 'pm10']:
            # PM2.5和PM10使用原有的日均值数据
            for station_id in station_ids:
                output_file = self.download_station_data(station_id, parameter, year_start, year_end, output_dir)
                if output_file:
                    output_files.append(output_file)
        else:
            # CO, O3, SO2, NO2使用小时数据
            for station_id in station_ids:
                output_file = self.download_station_hourly_data(station_id, parameter, year_start, year_end, output_dir)
                if output_file:
                    output_files.append(output_file)
                    
        return output_files

    def get_measurements(self, sensor_id, date_from, date_to, is_hourly=False):
        """获取测量数据"""
        endpoint = "measurements/hourly" if is_hourly else "measurements/daily"
        params = {
            'datetime_from': date_from,
            'datetime_to': date_to,
            'limit': 1000,
            'page': 1,
            'offset': 0
        }
        
        all_measurements = []
        total_pages = 1
        current_page = 1
        
        while current_page <= total_pages:
            params['page'] = current_page
            self.logger.info(f"获取数据: sensor={sensor_id}, endpoint={endpoint}, page={current_page}")
            
            response = self._make_request(f"{self.api_base}/sensors/{sensor_id}/{endpoint}", params=params)
            if not response:
                break
                
            try:
                data = response.json()
                if 'results' in data and data['results']:
                    self.logger.info(f"获取到 {len(data['results'])} 条记录")
                    all_measurements.extend(data['results'])
                    
                if 'meta' in data and 'found' in data['meta']:
                    total_records = data['meta']['found']
                    total_pages = (total_records + params['limit'] - 1) // params['limit']
                    self.logger.info(f"总记录数: {total_records}, 总页数: {total_pages}")
                    
                current_page += 1
                
            except Exception as e:
                self.logger.error(f'处理响应数据时出错: {str(e)}')
                break
                
        if all_measurements:
            try:
                # 将结果转换为DataFrame
                df = pd.DataFrame(all_measurements)
                
                # 处理日期时间字段 - 兼容 'date' 和 'datetime' 字段
                if 'date' in df.columns and isinstance(df['date'].iloc[0], dict):
                    # 如果 'date' 字段是字典类型（包含 'utc' 子字段）
                    df['datetime'] = pd.to_datetime([m['date']['utc'] for m in all_measurements])
                elif 'datetime' in df.columns:
                    # 如果已经有 'datetime' 字段
                    df['datetime'] = pd.to_datetime(df['datetime'])
                elif 'date' in df.columns:
                    # 如果 'date' 字段是字符串类型
                    df['datetime'] = pd.to_datetime(df['date'])
                else:
                    self.logger.error("数据中既没有 'date' 也没有 'datetime' 字段")
                    return pd.DataFrame()
                
                # 确保其他必需字段存在
                required_fields = {'value', 'unit', 'parameter'}
                if not all(field in df.columns for field in required_fields):
                    self.logger.error(f"数据缺少必需字段: {required_fields - set(df.columns)}")
                    return pd.DataFrame()
                
                # 只保留需要的列
                df = df[['datetime', 'value', 'unit', 'parameter']]
                
                self.logger.info(f"成功处理 {len(df)} 条记录")
                return df
                
            except Exception as e:
                self.logger.error(f"处理数据时出错: {str(e)}")
                self.logger.debug("数据结构示例:", df.head().to_dict() if 'df' in locals() else "无数据")
                return pd.DataFrame()
                
        return pd.DataFrame()

# 使用示例
# 设置 API key
# api_key = "389aba78fbec0010d6b8161319b1efe6848b9e0627eeb197071277edf52500c3"
# api_key = "0d689d7b2e847ef99dfa9d4ddde21e8fa239e33341f8c38b00c8f45857aea14c"
api_key = "7cd136e959fd56762f5f8e2e803012ab848148e002a603033607f7452c0a9e7c"


# 创建下载器实例
downloader = OpenAQDataDownloader(api_key=api_key)

# 站点列表
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

# 设置参数和年份范围
parameters = ['pm2.5','pm10','o3', 'co', 'so2', 'no2']  # 添加多个参数
# parameters = ['pm10']  # 添加多个参数
year_start = 2024  # 修改为仅2024年
year_end = 2024    # 修改为仅2024年

# 批量下载多个参数的数据
all_output_files = []
for parameter in parameters:
    print(f"\n开始下载 {parameter} 数据...")
    output_files = downloader.batch_download(station_ids, parameter, year_start, year_end)
    all_output_files.extend(output_files)

print("\n所有下载完成的文件列表:")
for f in all_output_files:
    print(f)