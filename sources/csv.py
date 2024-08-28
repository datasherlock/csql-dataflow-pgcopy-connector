class CSV:
    def __init__(self, ascii_delimiter: int, header: str, columns: str):
        self.delimiter: int = ascii_delimiter
        self.header = header.upper() if header.upper() in ['TRUE', 'FALSE'] else 'FALSE'
        self.columns = columns

    def __str__(self):
        return f"CSV(delimiter={self.delimiter}, header={self.header}, columns={self.columns})"
