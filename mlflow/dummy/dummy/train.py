# The data set used in this example is from http://archive.ics.uci.edu/ml/datasets/Wine+Quality
# P. Cortez, A. Cerdeira, F. Almeida, T. Matos and J. Reis.
# Modeling wine preferences by data mining from physicochemical properties. In Decision Support Systems, Elsevier, 47(4):547-553, 2009.
#
# The program is a copy of tutorial example from https://mlflow.org/docs/latest/tutorials-and-examples/tutorial.html
import logging
import sys
import warnings
from urllib.parse import urlparse

import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARN)
    logger = logging.getLogger(__name__)
    warnings.filterwarnings("ignore")

    np.random.seed(40)

    csv_url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
    try:
        data = pd.read_csv(csv_url, sep=";")
    except Exception as e:
        logger.exception("Unable to download training & test CSV, check your internet connection. Error: %s", e)
        sys.exit(1)

    alpha = float(sys.argv[1]) if len(sys.argv) > 1 else 0.5
    l1_ratio = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5

    train, test = train_test_split(data)

    train_x = train.drop(["quality"], axis=1)
    train_y = train[["quality"]]

    test_x = test.drop(["quality"], axis=1)
    test_y = test[["quality"]]

    mlflow.sklearn.autolog()

    with mlflow.start_run():
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        pred_y = lr.predict(test_x)

        rmse = np.sqrt(mean_squared_error(test_y, pred_y))
        mae = mean_absolute_error(test_y, pred_y)
        r2 = r2_score(test_y, pred_y)

        print(f"""
        ElasticNet model (alpha={alpha}, l1_ratio={l1_ratio}):
          RMSE: {rmse}
          MAE: {mae}
          R2: {r2}
        """)

        # mlflow.log_param("alpha", alpha)
        # mlflow.log_param("l1_ratio", l1_ratio)
        # mlflow.log_metric("rmse", rmse)
        # mlflow.log_metric("mae", mae)
        # mlflow.log_metric("r2", r2)

        tracking_url_type_store = urlparse(mlflow.get_tracking_uri()).scheme

        if tracking_url_type_store != "file":
            mlflow.sklearn.log_model(lr, "model", registered_model_name="ElasticNetWineModel")
        else:
            mlflow.sklearn.log_model(lr, "model")
