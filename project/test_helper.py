import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def create_mock_dataframe(num_rows = 1000):

    # Generate ID column
    ids = range(1, num_rows + 1)

    # Generate date_of_death column within date range
    start_date = datetime.strptime('01:01:2020', '%d:%m:%Y')
    end_date = datetime.strptime('30:09:2024', '%d:%m:%Y')

    date_of_death = [(start_date + timedelta(days=random.randint(0, (end_date - start_date).days))).strftime('%d:%m:%Y')
                     for _ in range(num_rows)]

    # Generate region column with random 5 letter strings
    region = [''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5)) for _ in range(num_rows)]

    # Generate gender column with 'm' or 'f'
    gender = [random.choice(['m', 'f']) for _ in range(num_rows)]

    # Generate diag column with 90% 'U071' and 10% 'U000'
    diag = ['U071' if random.random() < 0.9 else 'U000' for _ in range(num_rows)]

    df = pd.DataFrame({
        'id': ids,
        'date_of_death': date_of_death,
        'region': region,
        'gender': gender,
        'diag': diag
    })

    # Introduce about 10% missing values in 'gender' and 'region'
    for col in ['id', 'region']:
        df.loc[df.sample(frac=0.1).index, col] = np.nan

    return df

