import sqlalchemy
import google.auth
import google.auth.transport.requests
from google.cloud.sql.connector import Connector, IPTypes
import pg8000

from common.Logger import Logger


class GetConnection:
    def __init__(self, config):
        self.postgres_connection_string = None
        self.credentials = None
        self.engine = None
        self.connection = None
        self.db_password = None
        self.db_user = None
        self.db_name = None
        self.db_instance = None
        self.enable_iam_auth = True
        self.config = config
        self.logger = Logger().get_logger()

    def _get_iam_token(self):
        # Get the default application credentials
        self.credentials, project = google.auth.default()
        # Generate an OAuth 2.0 token for the IAM-based authentication
        self.credentials.refresh(google.auth.transport.requests.Request())
        return self.credentials.token

    def _get_connection(self):
        import pg8000
        config = self.config
        self.db_instance = config.get_config("cloudsql", "instance")
        self.db_name = config.get_config("cloudsql", "database")
        self.db_user = config.get_config("cloudsql", "user")
        self.db_password = config.get_config("cloudsql", "password")
        self.logger.info(f"Connecting to database instance - {self.db_instance}")
        connector = Connector()
        try:
            self.connection = connector.connect(
                instance_connection_string=self.db_instance,
                driver="pg8000",
                user=self.db_user,
                db=self.db_name,
                ip_type=IPTypes.PRIVATE,
                enable_iam_auth=self.enable_iam_auth,
                timeout=60,
            )
        except Exception as e:
            self.logger.error(e)
            raise e
        self.logger.info(f"Connected to database instance - {self.db_instance}")
        # iam_token = self.get_iam_token()
        # self.connection = pg8000.Connection(
        #     user=self.db_user,
        #     host="172.17.64.3",
        #     port="5432",
        #     database=self.db_name,
        #     password=iam_token,
        #     ssl_context=None  # Ensure the connection uses SSL
        # )
        return self.connection

    def get_engine(self):
        import pg8000
        self.engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=self._get_connection,
            echo=True,
            pool_size=100,  # Number of connections to maintain in the pool
            max_overflow=5,  # Number of connections to create beyond pool_size
            pool_timeout=30,  # Timeout for waiting to acquire a connection
            pool_recycle=1800  # Recycle connections after a certain number of seconds
        )
        return self.engine

    def _get_connection_string(self):
        return f"postgresql://{self.db_user}@{self.db_instance}/{self.db_name}?sslmode=disable"
