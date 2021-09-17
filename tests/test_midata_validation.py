import unittest
import pandas as pd
from midata.validation import RawHsbcMidataSchemaValidator, contains_numeric


class MidataTest(unittest.TestCase):
    def setUp(self):
        self.validator = RawHsbcMidataSchemaValidator()

    def test_missing_column_caught(self):
        data = {
                " Date": [],
                "Type": [],
                "Merchant/Description": [],
                "Debit/Credit": []
                }
        df = pd.DataFrame.from_dict(data)
        self.assertEqual(1, len(self.validator.validate_midata(df)))

    def test_non_numeric_column_caught(self):
        data = {
                " Date": ["00/00/0000"],
                "Type": ["xxxx"],
                "Merchant/Description": ["xxxx"],
                "Debit/Credit": ["1.323"],
                "Balance": ["xxxx"]
                }
        df = pd.DataFrame.from_dict(data)
        self.assertEqual(1, len(self.validator.validate_midata(df)))

    def test_extraction_from_series_not_possible(self):
        series = pd.Series(data=["a"], name="Debit/Credit")
        test_data = pd.Series(data=[False], name="Debit/Credit")
        should_equal = contains_numeric(series)
        try:
            pd.testing.assert_series_equal(should_equal, test_data, check_dtype=False)
        except AssertionError as e:
            raise self.failureException()

    def test_extraction_from_series_is_possible(self):
        series = pd.Series(data=["1.32"], name="Debit/Credit")
        test_data = pd.Series(data=[True], name="Debit/Credit")
        should_equal = contains_numeric(series)
        try:
            pd.testing.assert_series_equal(should_equal, test_data, check_dtype=False)
        except AssertionError as e:
            raise self.failureException()

    def test_monetary_extraction_from_series_is_possible(self):
        series = pd.Series(data=["£1.32", "£1000", "£ 9.12321", "$230030"], name="Debit/Credit")
        test_data = pd.Series(data=[True, True, True, True], name="Debit/Credit")
        should_equal = contains_numeric(series)
        try:
            pd.testing.assert_series_equal(should_equal, test_data, check_dtype=False)
        except AssertionError as e:
            raise self.failureException()


if __name__ == "__main__":
    unittest()
