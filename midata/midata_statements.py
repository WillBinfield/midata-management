import pandas as pd
from midata import RawHsbcMidataSchemaValidator, MidataValidationError, CleanedMidataSchemaValidator


class CleanedMidata:
    """
    Object holding midata data-set which has been cleaned.
    """

    data: pd.DataFrame

    def __init__(self, data: pd.DataFrame):

        validator = CleanedMidataSchemaValidator()
        errors = validator.validate_midata(data)
        if not len(errors) == 0:
            raise MidataValidationError(errors)
        else:
            self.data = data


class MidataArchive(CleanedMidata):
    """
    Object which merges cleaned midata data-sets into its own cleaned midata data-set
    """

    def __init__(self, df):
        super().__init__(df)

    def merge_child_data(self, midata_child: CleanedMidata):
        """
        Merges a CleanedMidata data-set into the MidataArchives data-set, only data from days not already in the
        master's data-set are merged.
        :param midata_child:
        :return:
        """
        already_present_days = self.data.Date.drop_duplicates().to_list()
        self.data = self.data.append(midata_child.data.loc[~midata_child.data.Date.isin(already_present_days)],
                                     verify_integrity=True,
                                     ignore_index=True)
        return self

    def archive(self, filepath):
        """
        Saves the archive data-set to the given path.
        :param filepath:
        :return: None
        """
        self.data.to_csv(filepath, index=False)


class RawMidata:
    """
    Object representing a valid raw midata data-set.
    """
    data: pd.DataFrame

    def __init__(self, data: pd.DataFrame):

        validator = RawHsbcMidataSchemaValidator()
        errors = validator.validate_midata(data)
        if not len(errors) == 0:
            raise MidataValidationError(errors)
        else:
            self.data = data

    def to_cleaned_data(self) -> CleanedMidata:
        """
        Transforms the raw midata data-set into a cleaned version.
        :return: CleanedMidata object representing the sanitised raw data.
        """
        clean_version = self.data

        clean_version.drop(labels=["Type", "Merchant/Description"], axis=1, inplace=True)
        clean_version.dropna(inplace=True)
        clean_version.rename(columns={"Debit/Credit": "Transactions"}, inplace=True)
        clean_version.rename(columns={" Date": "Date"}, inplace=True)

        clean_version["Transactions"].replace(regex=True, to_replace=r"[£\+]+", value=r"", inplace=True, )
        clean_version["Balance"].replace(regex=True, to_replace=r"[£\+]+", value=r"", inplace=True, )

        clean_version["Transactions"] = clean_version["Transactions"].astype(str).astype("float64").round(2)
        clean_version["Balance"] = clean_version["Balance"].astype(str).astype("float64").round(2)

        return CleanedMidata(clean_version)
