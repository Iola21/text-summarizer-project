import os
import zipfile
from pathlib import Path
from textSummarizer.logging import logger
from textSummarizer.utils.common import get_size
from textSummarizer.entity import DataIngestionConfig


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def validate_local_file(self):
        """Check if the local file exists and log its size."""
        if os.path.exists(self.config.local_data_file):
            file_size = get_size(Path(self.config.local_data_file))
            logger.info(f"Local file found: {self.config.local_data_file} (size: {file_size})")
        else:
            raise FileNotFoundError(f"Expected data file not found at {self.config.local_data_file}")

    def download_file(self):
        """Compatibility method: replaces actual download with local file validation."""
        self.validate_local_file()

    def extract_zip_file(self):
        """
        Extracts the zip file from local_data_file into the unzip_dir.
        """
        self.validate_local_file()
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)

        with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
            logger.info(f"Extracted {self.config.local_data_file} to {unzip_path}")
