import sqlalchemy
import google.auth
import google.auth.transport.requests
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
import logging
from common.Logger import Logger

# Set up logging with a specific format and level for detailed debug output
logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(name="google.cloud.sql.connector")
logger.setLevel(logging.DEBUG)


class GetConnection:
    """
    A class to manage database connections using Google Cloud SQL connector.

    This class supports IAM-based authentication and creates an SQLAlchemy engine for interacting with a PostgreSQL database.

    Attributes:
    - database_user: Username for connecting to the database.
    - database_name: Name of the database.
    - instance_connection_name: Google Cloud SQL instance connection string.
    """

    def __init__(self, database_user: str, database_name: str, instance_connection_name: str):
        """
        Initializes the connection parameters and sets up the Google Cloud SQL connector.

        Args:
        - database_user: The user to authenticate with the PostgreSQL database.
        - database_name: The name of the PostgreSQL database.
        - instance_connection_name: The instance connection string for the Google Cloud SQL instance.
        """
        # Set up connection-related attributes
        self.postgres_connection_string = None
        self.credentials = None
        self.engine = None
        self.connection = None
        self.db_password = None
        self.db_user = database_user
        self.db_name = database_name
        self.db_instance = instance_connection_name
        self.enable_iam_auth = True  # Assuming IAM-based authentication is always enabled

        # Logger for detailed output
        self.logger = Logger().get_logger()

        # Initialize the Google Cloud SQL Connector
        self.connector = Connector()

    def _get_iam_token(self) -> str:
        """
        Generates an OAuth 2.0 IAM token to authenticate with the Cloud SQL instance.
        This method is currently not used but is provided as a utility.

        Returns:
        - A string representing the IAM token.
        """
        ##################### UNUSED ###################
        # Get the default application credentials
        self.credentials, project = google.auth.default()

        # Generate an OAuth 2.0 token for the IAM-based authentication
        self.credentials.refresh(google.auth.transport.requests.Request())
        return self.credentials.token

    def _get_connection(self):
        """
        Establishes a connection to the Google Cloud SQL database using the Cloud SQL connector.

        Returns:
        - A raw connection object for the database.

        Raises:
        - Logs and re-raises exceptions if the connection attempt fails.
        """
        self.logger.info(f"Connecting to database instance - {self.db_instance}")
        try:
            # Use the Google Cloud SQL Connector to establish a database connection
            self.connection = self.connector.connect(
                instance_connection_string=self.db_instance,
                driver="pg8000",
                user=self.db_user,
                db=self.db_name,
                ip_type=IPTypes.PRIVATE,  # Use private IP for better security
                enable_iam_auth=self.enable_iam_auth,  # Use IAM-based authentication
                timeout=60  # Connection timeout in seconds
            )
        except Exception as e:
            # Log the error and re-raise it
            self.logger.error(f"Failed to connect to database instance - {self.db_instance}: {e}")
            raise e

        self.logger.info(f"Connected to database instance - {self.db_instance}")
        return self.connection

    def get_engine(self):
        """
        Creates and returns an SQLAlchemy engine that uses a custom connection creator.

        The connection is managed through Google Cloud SQL connector, allowing pooled connections.

        Returns:
        - An SQLAlchemy engine object.
        """
        self.logger.info("Creating SQLAlchemy engine...")
        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",  # Use the PostgreSQL dialect with the pg8000 driver
            creator=self._get_connection,  # Custom connection creator function
            echo=True,  # Enable SQLAlchemy logging for all statements executed (useful for debugging)
            pool_size=25,  # Number of connections to maintain in the pool
            max_overflow=5,  # Number of additional connections beyond pool_size if all connections are busy
            pool_timeout=60,  # Timeout (in seconds) for waiting to acquire a connection from the pool
            pool_recycle=1800  # Time (in seconds) after which connections are recycled to prevent stale connections
        )
        return self.engine

    def _get_connection_string(self) -> str:
        """
        Generates a connection string for the PostgreSQL database.

        Returns:
        - A string representing the connection URL.
        """
        return f"postgresql://{self.db_user}@{self.db_instance}/{self.db_name}?sslmode=disable"

