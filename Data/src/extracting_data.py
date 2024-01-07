import pandas as pd 

class DataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def read_csv(self):
        self.data = pd.read_csv(self.file_path, encoding='UTF-8')

    def convert_to_datetime(self, column):
        self.data[column] = pd.to_datetime(self.data[column])

    def sort_by_date(self, column):
        self.data = self.data.sort_values(column)