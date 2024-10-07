
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LinearRegression
import yaml
import os
import joblib

# обучаем модель
def fit_model():
	# 1 – чтение файла с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

	# 2 – загрузка результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
    data.drop(columns=['id', 'price'], inplace=True)

	# 3 – реализация основной логики шага с использованием гиперпараметров
    binary_features = data[['has_elevator', 'studio', 'is_apartment']]
    cat_features = data[['building_type_int']]
    num_features = data.select_dtypes(['float', 'int']).drop(columns=['building_type_int', 'target'])

    preprocessor = ColumnTransformer(
        [
        ('num', StandardScaler(), num_features.columns.tolist()),
        ('binary', OneHotEncoder(drop=params['one_hot_drop'], handle_unknown='ignore'), binary_features.columns.tolist()),
        ('cat', OneHotEncoder(handle_unknown='ignore'), cat_features.columns.tolist())
        ],
        remainder=params['remainder'],
        verbose_feature_names_out=False
    )

    model = LinearRegression()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data, data[params['target_col']])

	# 4 – сохранение обученной модели в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd) 

if __name__ == '__main__':
	fit_model()