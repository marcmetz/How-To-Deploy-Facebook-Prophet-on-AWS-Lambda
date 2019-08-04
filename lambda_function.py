import fbprophet
import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)


def lambda_handler(event, context):
    message = 'PD version: {}, NP version: {}, fbprophet version: {}'.format(
        pd.__version__,
        np.__version__,
        fbprophet.__version__
    )

    return {
        'message' : message
    }
