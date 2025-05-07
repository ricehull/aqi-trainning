
按照逻辑顺序对数据进行如下处理并进行训练评估推理，各个py文件都是可运行，承接上一步的处理。
1.	EPA定义的各种污染物的中断点
aqi_breakpoints.py

2.	从AWS S3获取gsod数据
retirval_gsod_data.py
noaa_data文件夹为获取的数据

3.	对gsod数据进行EDA的过程
eda_output文件夹为EDA输出的数据统计结果
eda_gsod_data.py

4.	根据EDA的结果进行gsod数据的处理（聚合，清洗，筛选等）
post_eda_gsod_data.py
post_eda_noaa_data文件夹为EDA后处理完毕的数据

5.	从openAQ获取各种污染物数据
retrival_openaq_data.py
openaq_data文件夹为openaq获取的原始数据

6.	整合污染物数据（从openAQ查询到的是单个的污染物数据）
integrate_pollutants.py
openaq_integrated_data按站整合的污染物数据

7.	计算污染物的aqi和空气的aqi值
calculate_aqi.py
aqi_data文件夹为各个站点的AQI数据

8.	整合单个站点的gsod数据和空气的AQI数据
merge_gsod_aqi_data.py

9.	整合所有站点的gsod数据和计算的空气的AQI数据（用于训练的数据）
merge_all_gsod_aqi_data.py
all_gsod_aqi_merged_data文件夹为输出数据

10.	使用autogluon进行训练，预测，推理和评估
aqi_predictor.py
autogluon_aqi_predictor文件夹为训练生成的模型，用于后续的预测