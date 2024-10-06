import pandas as pd

def remove_duplicates(data):
    feature_cols = data.columns.drop('building_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep='first')
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data

def fill_missing_values(data):
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index
    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)
    return data

def delete_outliers(data):
    num_cols = data.select_dtypes(['float', 'int']).drop(['building_id'], axis=1).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()
            
    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold*IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    data = data[~outliers]
    return data