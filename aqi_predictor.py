from autogluon.tabular import TabularPredictor
import pandas as pd
import os

class AQIPredictor:
    """
    使用AutoGluon对气象数据预测AQI。
    """
    def __init__(self, data_path, label='AQI', output_dir='autogluon_aqi_predictor', problem_type='regression'):
        self.data_path = data_path
        self.label = label
        self.output_dir = output_dir
        self.problem_type = problem_type
        os.makedirs(self.output_dir, exist_ok=True)
        self.predictor = None

    def train(self, time_limit=600):
        df = pd.read_csv(self.data_path)
        # 只用数值型特征和必要的辅助列
        feature_cols = [col for col in df.columns if col not in ['AQI', 'DATE', 'NAME', 'SITE']]
        train_data = df.dropna(subset=[self.label])
        self.predictor = TabularPredictor(label=self.label, path=self.output_dir, problem_type=self.problem_type)
        self.predictor.fit(train_data, time_limit=time_limit, presets='best_quality')
        print(f"模型训练完成，已保存到: {self.output_dir}")

    def evaluate(self, test_data_path=None):
        if self.predictor is None:
            self.predictor = TabularPredictor.load(self.output_dir)
        if test_data_path:
            test_data = pd.read_csv(test_data_path)
        else:
            test_data = pd.read_csv(self.data_path)
        test_data = test_data.dropna(subset=[self.label])
        perf = self.predictor.evaluate(test_data)
        print("评估结果:", perf)
        return perf

    def predict(self, input_df):
        if self.predictor is None:
            self.predictor = TabularPredictor.load(self.output_dir)
        preds = self.predictor.predict(input_df)
        print("预测结果:")
        print(preds)
        return preds
    
def predict_and_evaluate_on_new_data(new_data_path, true_aqi_col='AQI', predictor_dir='autogluon_aqi_predictor'):
    """
    使用训练好的模型对新数据进行预测，并与实际值进行MAE等评估。
    """
    from autogluon.tabular import TabularPredictor
    import pandas as pd
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    
    # 加载新数据
    df = pd.read_csv(new_data_path)
    # 只保留有真实AQI的行
    df = df.dropna(subset=[true_aqi_col])
    # 预测
    predictor = TabularPredictor.load(predictor_dir)
    X = df.drop(columns=[true_aqi_col])
    y_true = df[true_aqi_col].values
    y_pred = predictor.predict(X)
    # 评估
    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    r2 = r2_score(y_true, y_pred)
    print('预测结果:')
    print(y_pred)
    print('实际值:')
    print(y_true)
    print(f'MAE: {mae:.3f}, RMSE: {rmse:.3f}, R2: {r2:.3f}')
    return y_pred, y_true, mae, rmse, r2

# 运行示例
def execute_predictor():
    predictor = TabularPredictor.load('autogluon_aqi_predictor')
    new_data = pd.read_csv('all_gsod_aqi_merged_data/gsod_data_for_test.csv')
    predics = predictor.predict(new_data)
    print("预测完成, 结果:")
    print(predics)

def main():
    # # 1. 训练
    # predictor = AQIPredictor('all_gsod_aqi_merged_data/all_gsod_aqi_merged.csv')
    # predictor.train(time_limit=600)  # 可根据需要调整训练时间

    # # 2. 评估
    # predictor.evaluate()

    # # 3. 推理示例：用部分数据做预测
    # import pandas as pd
    # df = pd.read_csv('all_gsod_aqi_merged_data/all_gsod_aqi_merged.csv')
    # # 取前5行，去掉AQI列用于推理
    # input_df = df.drop(columns=['AQI']).head(20)
    # preds = predictor.predict(input_df)
    # print('推理结果:')
    # print(preds)

    # 4. 执行预测  
    # execute_predictor()
    # 5. 评估
    # predictor.evaluate('all_gsod_aqi_merged_data/all_gsod_aqi_merged.csv')

    # 6. 预测并评估新数据
    predict_and_evaluate_on_new_data('all_gsod_aqi_merged_data/gsod_data_for_test.csv')

if __name__ == '__main__':
    main()
