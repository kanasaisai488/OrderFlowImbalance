#engine/model_builder.py

from sklearn.linear_model import LinearRegression
import pandas as pd

def fit_linear_model(X, y):
    X = X.dropna()
    y = y.loc[X.index]
    model = LinearRegression().fit(X, y)
    coefs = pd.Series(model.coef_, index=X.columns)
    return model, coefs
