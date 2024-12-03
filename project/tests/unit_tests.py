import unittest

import numpy as np
import pytest
# TODO install pytest?

import pandas as pd
from project.tests import test_helper
from project.transformation import *


class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.num_rows = 1000
        self.df = test_helper.create_mock_dataframe(1000)



    def test_filter_transform_to_datetime(self):
        # Check the data type of the date_of_death column before conversion
        self.assertFalse(pd.api.types.is_datetime64_any_dtype(self.df['date_of_death']))

        # Convert the date_of_death column to a datetime object
        transformed_df = filter_transform_to_datetime(self.df, column='date_of_death')

        # Assert that the date_of_death column is now a datetime object
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transformed_df['date_of_death']))

    def test_filter_rows_by_values(self):

        # Check the number of rows before filtering
        self.assertEqual(len(self.df), self.num_rows)

        # Filter all rows where diag is not 'U071'
        transformed_df = filter_rows_by_values(self.df, 'diag', 'U071')

        # Assert that there are about 90% of the rows left (with a margin of 1%)
        self.assertAlmostEqual(len(transformed_df), np.floor(self.num_rows * 0.9), delta=self.num_rows * 0.01)

    def test_filter_handle_missing_values(self):

        # Check that the number of missing values before filtering is greater than 0
        self.assertGreater(self.df['id'].isnull().sum(), 0)

        # Remove all missing values in the id column
        for strategy in Strategy:
            transformed_df = filter_handle_missing_values(self.df, 'id', strategy=strategy)
            self.assertEqual(transformed_df['id'].isnull().sum(), 0)

    def test_filter_drop_columns(self):

        # Check that the number of columns is correct before dropping
        self.assertEqual(len(self.df.columns), 5)

        # Drop columns id gender and region
        transformed_df = filter_drop_columns(self.df, ['diag', 'date_of_death'])
        self.assertEqual(len(transformed_df.columns), 2)

        # Assert that date_of_death and diag are still present
        self.assertIn('diag', transformed_df.columns)
        self.assertIn('date_of_death', transformed_df.columns)

if __name__ == '__main__':
    unittest.main()
