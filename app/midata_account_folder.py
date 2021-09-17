import pandas as pd
import os
import pathlib
import logging
from csv import writer
from midata import RawMidata, MidataArchive

# refactor to have logger per instance with errors on it.
logger = logging.getLogger("midata_app")


class MidataAccountFolder:
    """
    Transforms and archives all statements within an account folder into a master copy.
    """

    account_folder_path: str
    master_archive_file: str
    statements: str
    master_archive: MidataArchive

    def __init__(self, path: str):
        self.account_folder_path = path
        self.master_archive_file = os.path.join(self.account_folder_path, "midata_master_copy.csv")
        self.statements = os.path.join(self.account_folder_path, "statements")
        if not self.master_file_exists():
            self.create_master_file()
        self.master_archive = MidataArchive(pd.read_csv(self.master_archive_file))

    def process_record_file(self, file: str) -> None:
        """
        Archives data extracted from a midata statement.
        :param file:
        :return: None
        """
        child_midata = RawMidata(pd.read_csv(file)).to_cleaned_data()

        self.master_archive \
            .merge_child_data(child_midata) \
            .archive(self.master_archive_file)

    def is_valid(self, path) -> bool:
        """
        Validates the given path is a csv file for which archiving may be possible.
        :param path:
        :return: bool indicating the given path is a csv file.
        """
        file = pathlib.Path(path)
        return (file.is_file() and file.suffix == ".csv")  # does this work?

    def process_account_folder(self) -> None:
        """
        Kicks off archiving process for all valid statements within the account folder.
        :return:
        """
        for file_name in os.listdir(self.statements):
            file_path = os.path.join(self.statements, file_name)
            if self.is_valid(file_path):
                self.process_record_file(file_path)

    def master_file_exists(self) -> bool:
        """
        Validates that a master midata copy already exists.
        :return: bool to indicate existence.
        """
        return os.path.exists(self.master_archive_file)

    def create_master_file(self) -> None:
        """
        Creates an empty midata master copy.
        :return: None
        """
        with open(self.master_archive_file, "w") as master_file:
            master_file_writer = writer(master_file)
            master_file_writer.writerow(["Date", "Transactions", "Balance"])
