import os
import pathlib
import sys
import logging
from midata import MidataValidationError
from app import MidataAccountFolder


logger = logging.getLogger("midata_app")
logger.setLevel(logging.DEBUG)
logger_output_path = os.path.join(os.path.dirname(__file__), "midata_log.log")
logger.addHandler(logging.FileHandler(logger_output_path, mode="w"))
logger.addHandler(logging.StreamHandler(sys.stdout))


class MidataApp:
    """
    Orchestrates the process of creating midata account statement archivers for each account directory in the root path.
    """

    root: str
    accounts_in_root: list

    def __init__(self, path: str):
        self.root = path #passing in non directory root?
        self.accounts_in_root = []

    @staticmethod
    def valid_account_folder(path) -> bool:
        """
        Only directories within the root directory will are valid for archiving.
        ;:return: A boolean to indicate the path is a directory.
        """

        path = pathlib.Path(path)
        return path.is_dir()

    def find_accounts_in_root(self):
        """
        Each valid directory within the root directory is stored. I hate this particularly.
        :return: None
        """
        for account_name in os.listdir(self.root):
            account_path = os.path.join(self.root, account_name)
            if self.valid_account_folder(account_path):
                self.accounts_in_root.append(account_path)

    @staticmethod
    def try_archive_account(account_path):
        """
        Attempts to kick off the account folder archiving process.
        :param account_path:
        :return: None
        """
        logger.info(f"Attempting to archive account at path {account_path}")
        try:
            midata_account_folder = MidataAccountFolder(account_path)
            midata_account_folder.process_account_folder()
            logger.info("Account archived successfully")
        except MidataValidationError as e:
            # more detail - account/file
            logger.info(f"A data-set for the account at path '{account_path}' was not in a valid midata format"
                        f" due to below reasons therefore the account has been skipped. - \n"
                        f"{e.validation_errors_as_string()}")

    def execute(self):
        """
        All valid directories within the root directory are passed to an archiving attempt.
        :return: None
        """
        logger.info("Beginning archiving...")
        self.find_accounts_in_root()
        for account_path in self.accounts_in_root:
            self.try_archive_account(account_path)


