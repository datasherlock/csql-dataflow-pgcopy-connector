import apache_beam as beam
from datetime import datetime


class CSV:
    """
    Represents the structure of a CSV file with configurable delimiter, quote character, header, and columns.
    """

    def __init__(self, ascii_delimiter: int, ascii_quote: int, header: str, columns: str):
        # The ASCII value for the delimiter character (e.g., 44 for comma, 29 for group separator)
        self.delimiter: int = ascii_delimiter

        # If 'header' is either 'TRUE' or 'FALSE' (case insensitive), set it accordingly. Defaults to 'FALSE' otherwise.
        self.header: str = header.upper() if header.upper() in ['TRUE', 'FALSE'] else 'FALSE'

        # A string representing the column names in the CSV.
        self.columns: str = columns

        # The ASCII value for the quote character (e.g., 34 for double quote)
        self.quote: int = ascii_quote

    def __str__(self) -> str:
        """
        Returns a string representation of the CSV configuration.
        """
        return f"CSV(delimiter={self.delimiter}, quote={self.quote}, header={self.header}, columns={self.columns})"


class ExtractPartitionKey(beam.DoFn):
    """
    A DoFn to extract the partition key from each element in a PCollection.

    Partition key is determined based on the timestamp found in the element.
    """

    def __init__(self, args):
        # Delimiter used to split fields in the element
        self.delimiter: int = args.delimiter

        # Header information, although not used in partitioning
        self.header: str = args.header

        # Columns in the CSV, not used in this transform but could be used for reference
        self.columns: str = args.column_list

        # Quote character, not used directly in the partitioning logic
        self.quote: int = args.quote

    def process(self, element: str):
        """
        Processes each element to extract the partition key based on the first field.

        :param element: A string representing a line in the CSV file.
        :yield: A tuple containing the partition key and the element itself.
        """
        # Split the element into fields using the specified delimiter (converted from ASCII value to character)
        fields = element.split(chr(self.delimiter))

        # Parse the timestamp from the first field to determine the partition key
        # Assuming the first field has a datetime in the format "%Y-%m-%d %H:%M:%S"
        partition_field = datetime.strptime(fields[0], "%Y-%m-%d %H:%M:%S")

        # Construct the partition key in the format "year_month_day"
        partition_key = f"{partition_field.year}_{partition_field.month:02d}_{partition_field.day:02d}"

        # Yield the partition key along with the original element
        yield partition_key, element
