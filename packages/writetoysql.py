from .helpers import read_secret_params
from apache_beam import DoFn
import logging
FORMAT = '%(asctime)s %(module)s %(funcName)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class WriteToYSQL(DoFn):
    def __init__(self, table, database, project_id, port, user_secret_name,
                 password_secret_name, host_secret_name, ca_cert_secret_name):
        self.table = table
        self.database = database
        self.tmp_ca_cert = '/tmp/ybca.crt'
        self.sql_port = port
        self.sql_user = read_secret_params(
            project_id, user_secret_name)
        self.sql_pass = read_secret_params(
            project_id, password_secret_name)
        self.sql_host = read_secret_params(
            project_id, host_secret_name)
        self.sql_ca_cert = read_secret_params(
            project_id, ca_cert_secret_name)

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
        import pg8000.dbapi
        import logging
        try:
            self.create_file(self.tmp_ca_cert, self.sql_ca_cert)
            ssl_context = SSLContext(PROTOCOL_TLSv1_2)
            ssl_context.load_verify_locations(self.tmp_ca_cert)
            ssl_context.verify_mode = CERT_REQUIRED
            self.connection = pg8000.dbapi.connect(
                user=self.sql_user,
                password=self.sql_pass,
                database=self.database,
                host=self.sql_host,
                port=self.sql_port,
                ssl_context=ssl_context
            )
            self.cursor = self.connection.cursor()
        except Exception as e:
            logging.info("YSQL Setup failed: " + str(e))
            raise e

    def teardown(self):
        self.connection.close()

    # Commit batch in finish bundle, this is not needed for TCQL
    def finish_bundle(self):
        self.connection.commit()

    # Device is primary key, whose status is updated/created in this function. Hence 'INSERT...ON CONFLICT' is used.
    def process(self, elements):
        import json
        from math import trunc
        insert_stmt = f"""INSERT INTO {self.table} (id,eventtime,value,base) 
                values (%s,to_timestamp(%s/1000.),%s,%s)"""
        _, rows = elements
        logging.info("Recieved anomaly data: " + str(rows))
        try:
            for element in rows:
                logging.info("Processing element: " + str(element))
                data, distance = element
                k, v = data
                json_element = json.loads(v)
                self.cursor.execute(insert_stmt, (k, json_element['eventtime'], json_element['value'], json_element['base']))
        except Exception as e:
            logging.info(
                "YSQL: Insertion failed for record: {} with error: {}".format(elements,e))
            raise e
