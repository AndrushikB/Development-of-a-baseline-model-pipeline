
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
import joblib
import json
import yaml
import os

# оценка качества модели
def evaluate_model():
	
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

	
    pipeline = joblib.load('models/fitted_model.pkl')
    data = pd.read_csv('data/initial_data.csv')

	
    cv_strategy = StratifiedKFold(n_splits=params['n_splits'])
    cv_res = cross_validate(
        pipeline,
        data,
        data[params['target_col']],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['metrics']
        )
    for key, value in cv_res.items():
        cv_res[key] = value.mean()
        
	
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as fd:
        json.dump(cv_res, fd)

if __name__ == '__main__':
	evaluate_model()