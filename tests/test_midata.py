import unittest
import pandas as pd
from midata.midata_statements import RawMidata, MidataArchive, CleanedMidata
from midata.validation import MidataValidationError


class MidataTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_invalid_construction_fails_non_numeric(self):
        df = pd.DataFrame({
            " Date": ["00/00/0000"],
            "Type": ["xxxx"],
            "Merchant/Description": ["xxxx"],
            "Debit/Credit": ["1.323"],
            "Balance": ["xxxx"]
        })

        with self.assertRaises(MidataValidationError):
            RawMidata(df)

    def test_invalid_construction_fails_missing_column(self):
        df = pd.DataFrame({
            "Type": ["xxxx"],
            "Merchant/Description": ["xxxx"],
            "Debit/Credit": ["1.323"],
            "Balance": ["xxxx"]
        })

        with self.assertRaises(MidataValidationError):
            RawMidata(df)

    def test_valid_construction_suceeds(self):
        df = pd.DataFrame({
            " Date": ["00/00/0000"],
            "Type": ["xxxx"],
            "Merchant/Description": ["xxxx"],
            "Debit/Credit": ["1.323"],
            "Balance": ["£1000"]
        })

        try:
            RawMidata(df)
        except MidataValidationError:
            self.fail()

    def test_rawmidata_cleaned_successfully(self):
        df = pd.DataFrame({
            " Date": ["00/00/0000"],
            "Type": ["xxxx"],
            "Merchant/Description": ["xxxx"],
            "Debit/Credit": ["1.3238"],
            "Balance": ["£1000"]
        })

        should_equal = pd.DataFrame({
            "Date": ["00/00/0000"],
            "Transactions": [1.32],
            "Balance": [1000]
        })

        try:
            cleaned_data = RawMidata(df).to_cleaned_data().data
            pd.testing.assert_frame_equal(should_equal, cleaned_data, check_dtype=False)
        except MidataValidationError as e:
            for error in e.validation_errors:
                print(error)
            raise self.failureException()
        except AssertionError:
            raise self.failureException()

    def test_archive_child_merge_successful(self):
        archive = MidataArchive(pd.DataFrame({
            "Date": ["00/00/0001"],
            "Transactions": [5],
            "Balance": [2000]
        }))

        child = CleanedMidata(pd.DataFrame({
            "Date": ["00/00/0000"],
            "Transactions": [1.32],
            "Balance": [1000]
        }))

        should_equal = pd.DataFrame({
            "Date": ["00/00/0001", "00/00/0000"],
            "Transactions": [5, 1.32],
            "Balance": [2000, 1000]
        })

        try:
            pd.testing.assert_frame_equal(archive.merge_child_data(child).data, should_equal, check_dtype=False)
        except AssertionError:
            raise self.failureException()


if __name__ == "__main__":
    unittest()
