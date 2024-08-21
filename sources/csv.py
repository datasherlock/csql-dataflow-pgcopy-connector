
class CSV:
    def __init__(self, ascii_delimiter: int, header: str, columns: str):
        self.delimiter: int = int(ascii_delimiter)
        self.header = 'FALSE'
        self.columns = columns

