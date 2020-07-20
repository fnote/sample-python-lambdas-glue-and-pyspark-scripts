import pandas as pd
import numpy as np
import pytest
from src.pa import pa_etl_script


def test_pa_etl_job():
    data = {'Item ID': [np.NaN, '2505711'],
            'Zone ID': ['1', '2'],
            'New price effective date': ['15.41', '2'],
            'Reason code': ['Reduce % price change', 'Correct inventory positioning'],
            'Export date': ['5/14/2020', '5/14/2020']}

    df = pd.DataFrame(data)
    with pytest.raises(Exception):
        pa_etl_script.validate_data(df)
