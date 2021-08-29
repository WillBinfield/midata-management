import pandas as pd
from typing import List
from pandas_schema import Column, Schema
from pandas_schema.validation import  CanConvertValidation, ValidationWarning
from sys import exit


class MidataValidationError(ValueError):

    def __init__(self, errors: List[ValidationWarning]):
        Exception.__init__(self, "Not a valid midata data-set.")
        self.validation_errors = errors


class MidataSchemaValidator:

    def __init__(self):
        pass

    def validate_midata(self, df):
        pass


class HsbcMidataSchemaValidator:

    schema = Schema([
        Column(" Date", [CanConvertValidation(str)]),
        Column("Type", [CanConvertValidation(str)]),
        Column("Merchant/Description", [CanConvertValidation(str)]),
        Column("Debit/Credit", [CanConvertValidation(str)]),
        Column("Balance", [CanConvertValidation(str)])
    ])

    def __init__(self):
        pass

    def validate_midata(self, df):
        return self.schema.validate(df)


class CleanedMidata:
    data: pd. DataFrame

    def __init__(self, df: pd.DataFrame):
        self.data = df
        self.set_date_column()
        self.set_transaction_column()
        self.set_balance_column()

    def set_date_column(self):
        self.date_column = self.data['Date']

    def set_transaction_column(self):
        self.transaction_column = self.data['Transactions']

    def set_balance_column(self):
        self.balance_column = self.data['Balance']


class MidataArchive(CleanedMidata):

    def __init__(self, df):
        super().__init__(df)

    def merge_child_data(self, midata_child: CleanedMidata):
        already_present_days = self.date_column.drop_duplicates().to_list()
        self.data = self.data.append(midata_child.data.loc[~midata_child.date_column.isin(already_present_days)], verify_integrity=True, ignore_index=True)


class RawMidata:

    def __init__(self, data: pd.DataFrame, validator: HsbcMidataSchemaValidator):

        errors = validator.validate_midata(data)
        if not len(errors) == 0:
            raise MidataValidationError(errors)
        else:
            self.data = data


    def to_cleaned_data(self) -> CleanedMidata :
        clean_version = self.data

        clean_version.drop(labels=["Type", "Merchant/Description"], axis=1, inplace=True)
        clean_version.dropna(inplace=True)
        clean_version.rename(columns={"Debit/Credit": "Transactions"}, inplace=True)
        clean_version.rename(columns={" Date": "Date"}, inplace=True)

        clean_version["Transactions"].replace(regex=True, to_replace=r"[£\+]+", value=r"", inplace=True, )
        clean_version["Balance"].replace(regex=True, to_replace=r"[£\+]+", value=r"", inplace=True, )

        clean_version["Transactions"].astype("float64").round(2)
        clean_version["Balance"].astype("float64").round(2)

        return CleanedMidata(clean_version)


def main():

    archive = MidataArchive(pd.read_csv('/Users/willbinfield/ProperProjects/career/midata_master.csv'))

    try:
        midata_child = RawMidata(pd.read_csv('/Users/willbinfield/ProperProjects/career/midata_child.csv'), HsbcMidataSchemaValidator())
        archive.merge_child_data(midata_child.to_cleaned_data())
        print(archive.data)

    except MidataValidationError as e:
        for error in e.validation_errors:
            print(error)



if __name__ == '__main__':
    main()