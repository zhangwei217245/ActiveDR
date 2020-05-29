import pandas as pd

class CSVReader(object):
    def _print_row(self, row):
        print(row)

    def __init__(self, csv_path, on_row,  **kwargs):
        self.source = csv_path
        self.on_row = self._print_row if on_row==None else on_row
        self.kwargs = kwargs

    def load_csv(self):
        self.dataframe = pd.read_csv(self.source, **self.kwargs)
        return self.get_data_frame()

    def iter_csv_rows(self):
        for row in self.dataframe.itertuples():
            self.on_row(row)

    def get_data_frame(self):
        return self.dataframe