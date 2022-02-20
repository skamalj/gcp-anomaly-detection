import apache_beam as beam
import logging
logger = logging.getLogger(__name__)

# Read DB connect parameters from GCP Secret Manager
def read_secret_params(project_id, secret_id):
    from google.cloud import secretmanager
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest" 
    return client.access_secret_version(request={"name": secret_name}).payload.data.decode("UTF-8")

def train_model(element, limit):
        from google.cloud import bigquery
        from datetime import datetime, timezone

        client = bigquery.Client()
        key_id,_ = element
        model_name = "model_{}_{}".format(key_id, datetime.now(timezone.utc).strftime('%M'))
        create_model_stmt = """CREATE OR REPLACE MODEL pubsub.{} OPTIONS( MODEL_TYPE = 'kmeans',
            NUM_CLUSTERS = 1, KMEANS_INIT_METHOD = 'kmeans++' ) AS
            SELECT  value as bp FROM pubsub.patient_raw_data
            WHERE device_id = {} ORDER BY eventtime DESC LIMIT {}""".format(model_name,key_id, limit)
        client.query(create_model_stmt).result()
        return (key_id, model_name)

def is_anomolous(element, models):
        from google.cloud import bigquery
        import json, math

        client = bigquery.Client()
        key_id, val = element
        bp = math.trunc(json.loads(val)['value'])
        if key_id in models:
                model_name = models[key_id]
                create_model_stmt = """SELECT * FROM  ML.DETECT_ANOMALIES(MODEL `pubsub.{}`,
                            STRUCT(0.01 AS contamination), (SELECT  {} as bp))""".format(model_name,bp)
                rows = client.query(create_model_stmt).result()
                df = rows.to_dataframe()
                result = df['normalized_distance'].values.tolist()[0]
                return (element, result)
        else:
                return (element, 0)

def raw_to_tuple(element):
        import json
        from math import trunc
        json_element = json.loads(element.decode('utf-8'))
        return (json_element['id'], json_element['eventtime'], trunc(json_element['value']))

def element_from_kv(element):
        import json
        from math import trunc
        _,v = element
        json_element = json.loads(v)
        return (trunc(json_element['value']), json_element['id'], json_element['eventtime'])

def update_distance(element):
        import json
        from math import trunc
        data, distance = element
        k, v = data
        json_element = json.loads(v)
        return (distance, json_element['id'], json_element['eventtime'])

# Create a KV pair for each event. This is needed for stateful DoFns as state works on each key independently.
def create_kv_events(element):
    import json
    json_element = json.loads(element.decode('utf-8'))
    return (json_element['id'], json.dumps(json_element))

def parse_simulated_events(element):
    import json
    json_element = json.loads(element.decode('utf-8'))
    base_value = json_element['source']['base'] if json_element['isanomaly'] == 1  else json_element['source']['base']*1.1
    value = base_value + json_element['variation']
    data = {'id': json_element['source']['id'], 'eventtime' : json_element['eventtime'], 'value' : value, 'base': json_element['source']['base'] }
    return json.dumps(data).encode('utf-8')

class AddTimestampDoFn(beam.DoFn):
  def process(self, element):
      import json
      _, v = element
      unix_timestamp = json.loads(v)['eventtime']/1000.0
      yield beam.window.TimestampedValue(element, unix_timestamp)

class LogEventsWithTimestampAndWindow(beam.DoFn):
    
    def __init__(self, message_prefix):
        self.message_prefix = message_prefix

    def process(self, element, window=beam.DoFn.WindowParam, timestamp=beam.DoFn.TimestampParam):
        from apache_beam.transforms.window import Timestamp
        logger.info("{}: Event: {}, Timestamp: {}, Window: {}, Current Time {}".format(
            self.message_prefix, element, Timestamp.to_utc_datetime(timestamp),  window, Timestamp.to_utc_datetime(Timestamp.now())))
