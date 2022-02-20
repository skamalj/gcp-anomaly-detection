from .helpers import read_secret_params
from apache_beam import DoFn
import logging
FORMAT = '%(asctime)s %(module)s %(funcName)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ReadFromYCQL(DoFn):
    def __init__(self, table, project_id, keyspace, cql_port, cql_user_secret_name,
                 cql_password_secret_name, cql_host_secret_name, cql_ca_cert_secret_name, limit):
        self.table = table
        self.keyspace = keyspace
        self.tmp_ca_cert = '/tmp/ybca.crt'
        self.cql_port = cql_port
        self.cql_user = read_secret_params(
            project_id, cql_user_secret_name)
        self.cql_pass = read_secret_params(
            project_id, cql_password_secret_name)
        self.cql_host = read_secret_params(
            project_id, cql_host_secret_name)
        self.cql_ca_cert = read_secret_params(
            project_id, cql_ca_cert_secret_name)
        self.limit = limit

    # Function needed to create CA cert file in tmp folder for databases
    # CA CRT value is save in Secret Mamnager, the data is retrieved as string and saved as file
    def create_file(self, file_path, data):
        from os.path import exists
        import time
        if not exists(file_path):
            with open(file_path, "w") as file:
                file.write(data)
        while not exists(file_path):
            time.sleep(3)

    def setup(self):
        from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
        from cassandra.cluster import Cluster
        from cassandra.policies import RoundRobinPolicy
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.query import dict_factory
        import time, random

        try:

            ssl_context = SSLContext(PROTOCOL_TLSv1_2)
            self.create_file(self.tmp_ca_cert, self.cql_ca_cert)
            ssl_context.load_verify_locations(self.tmp_ca_cert)
            ssl_context.verify_mode = CERT_REQUIRED
            auth_provider = PlainTextAuthProvider(
                username=self.cql_user, password=self.cql_pass)
            cluster = Cluster([self.cql_host], protocol_version=4,
                              load_balancing_policy=RoundRobinPolicy(),
                              ssl_context=ssl_context, auth_provider=auth_provider,
                              port=self.cql_port, connect_timeout=10)
            self.session = cluster.connect()
            self.session.row_factory = dict_factory
        except Exception as e:
            logger.info("YCQL Setup failed While Reading Training Data: " + str(e))
            time.sleep(random.randint(1,3))
            raise e

    def teardown(self):
        self.session.shutdown()

    def process(self, element, timestamp=DoFn.TimestampParam):
        import json
        import math
        
        k, v = element
        key_id = k
        select_stmt = f"SELECT device_id, value, eventtime from {self.keyspace}.{self.table} where device_id = ? and value != null ORDER BY eventtime DESC LIMIT {self.limit}"
        try:
            logger.info("Reading from YCQL for {}".format(k))
            prepared_stmt = self.session.prepare(select_stmt)
            index = 0
            for row in self.session.execute(prepared_stmt, (key_id,)):
                # convert json to string
                index+=1
                row['timestamp'] = float(timestamp)
                yield ((key_id , math.trunc(index/100)), json.dumps(row))
        except Exception as e:
            logger.error("Reading Training Data: Failed to get records for ID: " + str(key_id))
            raise e
