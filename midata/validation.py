import pandas as pd
from typing import List
from pandas_schema import Column, Schema
from pandas_schema.validation import CanConvertValidation, ValidationWarning, CustomSeriesValidation


def contains_numeric(unvalidated_series: pd.Series) -> pd.Series:
    """
    Validates whether each element of a Series contains digits.
    :param unvalidated_series:
    :return: A Boolean Series of the same dimension indicating if the digits were found.
    """
    return unvalidated_series.str.contains(r"\d", case=False, na=True)


class MidataValidationError(ValueError):
    """
    Indicates that a dataframe is does not match needed criteria for a midata data-set.
    """
    def __init__(self, errors: List[ValidationWarning]):
        Exception.__init__(self, "Not a valid midata data-set.")
        self.validation_errors = errors

    def validation_errors_as_string(self) -> str:
        """
        Formats individual ValidationWarnings into string.
        :return: Returns a string with ValidationWarnings split onto their own line.
        """
        errors = [f"{error}\n" for error in self.validation_errors]

        return "".join(errors)


class RawHsbcMidataSchemaValidator:
    """
    Validates that a dataframe contains the needed criteria for a midata data-set.
    """

    schema = Schema([
        Column(" Date",
               [CanConvertValidation(str)]),
        Column("Type",
               [CanConvertValidation(str)]),
        Column("Merchant/Description",
               [CanConvertValidation(str)]),
        Column("Debit/Credit",
               [CanConvertValidation(str),
                CustomSeriesValidation(contains_numeric, "A number couldn't be extracted")]),
        Column("Balance",
               [CanConvertValidation(str),
                CustomSeriesValidation(contains_numeric, "A number couldn't be extracted")])
    ])

    def __init__(self):
        pass

    def validate_midata(self, df) -> List[ValidationWarning]:
        """
        Validates that a given dataframe matches the needed criteria.
        :param df:
        :return: A list of individual validation errors that indicate the dataframe is invalid.
        """
        return self.schema.validate(df)


class CleanedMidataSchemaValidator:

    schema = Schema([
        Column("Date",
                [CanConvertValidation(str)]),
        Column("Transactions",
                [CanConvertValidation(float)]),
        Column("Balance",
                [CanConvertValidation(float)])
    ])

    def __init__(self):
        pass

    def validate_midata(self, df) -> List[ValidationWarning]:
        return self.schema.validate(df)
