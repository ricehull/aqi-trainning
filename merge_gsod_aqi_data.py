import os
import pandas as pd

# 站点编号到站点名称的映射
site_id_to_name = {
    "41640099999": "lahore-pk",
    "70261026411": "fairbanks",
    "72278023183": "bakersfield",
    "72494693232": "fresno",
    "72287493134": "los-angeles",
    "72384023155": "phoenix",
    "72389093193": "san-jose",
    "72389693144": "visalia"
}

class MergeGSODAQIDataProcessor:
    """
    合并NOAA气象数据和AQI数据，按站点和日期合并，输出合并文件。
    """
    def __init__(self, gsod_dir='post_eda_noaa_data', aqi_dir='aqi_data', output_dir='gsod_aqi_merge_data'):
        self.gsod_dir = gsod_dir
        self.aqi_dir = aqi_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def merge_for_all_sites(self):
        for gsod_file in os.listdir(self.gsod_dir):
            if not gsod_file.endswith('.csv'):
                continue
            site_id = self._extract_site_id(gsod_file)
            site_name = site_id_to_name.get(site_id)
            if not site_name:
                print(f'未找到编号 {site_id} 的站点名称，跳过')
                continue
            aqi_file = self._find_aqi_file(site_name)
            if aqi_file:
                self.merge_single_site(gsod_file, aqi_file, site_name)
            else:
                print(f'未找到站点 {site_name} 的AQI数据，跳过')

    def merge_single_site(self, gsod_file, aqi_file, site_name):
        gsod_path = os.path.join(self.gsod_dir, gsod_file)
        aqi_path = os.path.join(self.aqi_dir, aqi_file)
        gsod_df = pd.read_csv(gsod_path)
        aqi_df = pd.read_csv(aqi_path)
        # 以日期合并，NOAA用'DATE'，AQI用'datetime'
        merged = pd.merge(gsod_df, aqi_df[['datetime', 'overall_aqi']], left_on='DATE', right_on='datetime', how='inner')
        merged = merged.drop(columns=['datetime'])
        merged = merged.rename(columns={'overall_aqi': 'AQI'})
        out_name = f'{site_name}_gsod_aqi_merged.csv'
        out_path = os.path.join(self.output_dir, out_name)
        merged.to_csv(out_path, index=False)
        print(f'合并完成: {out_path}')

    def _extract_site_name(self, gsod_file):
        # 假设NOAA文件名格式为 post_eda_{site}_xxxx.csv 或 station_{site}_xxxx.csv
        name = gsod_file.replace('post_eda_', '').replace('station_', '').split('_')[0]
        return name.lower()

    def _extract_site_id(self, gsod_file):
        # 假设NOAA文件名格式为 station_{siteid}_xxxx.csv 或 post_eda_station_{siteid}_xxxx.csv
        # 先去掉前缀
        name = gsod_file.replace('post_eda_', '').replace('station_', '')
        # 取第一个下划线前的编号
        site_id = name.split('_')[0]
        return site_id

    def _find_aqi_file(self, site_name):
        # 在aqi_data目录下查找包含site_name的文件
        for f in os.listdir(self.aqi_dir):
            if site_name in f and f.endswith('.csv'):
                return f
        return None

def main():
    processor = MergeGSODAQIDataProcessor()
    processor.merge_for_all_sites()

if __name__ == '__main__':
    main()
