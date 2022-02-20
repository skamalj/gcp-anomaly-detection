import apache_beam as beam
from google.cloud import bigquery
import logging

FORMAT = '%(asctime)s %(module)s %(funcName)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class WriteToBQ(beam.CombineFn):
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def create_accumulator(self):
      return ([], 0)
  
    def add_input(self, rows_count, input):
      (rows, count) = rows_count
      rows.append(input)
      return rows , count + 1
  
    def merge_accumulators(self, accumulators):
      accum_rows = []
      count = 0
      for accumulator in accumulators:
          (rows, c) = accumulator
          count += c
          accum_rows.extend(rows)
      return (accum_rows, count)
  
    def extract_output(self, rows_count):
      rows, _ = rows_count
      return self.write_to_bq(rows)
  
    def write_to_bq(self,rows):
        import json
        client = bigquery.Client()
        rows_json = [json.loads(j) for j in rows]
        insert_stmt = """INSERT  {}.{}.{} (device_id, value, eventtime, timestamp) 
          SELECT id,val,eventtime, CURRENT_TIMESTAMP()
          FROM UNNEST(ARRAY<STRUCT<id integer, val integer, eventtime INT64, timestamp float64>>{} )""".format(
            self.project_id, self.dataset_id, self.table_id,[tuple(j.values()) for j in rows_json])
        client.query(insert_stmt).result()
        logger.info("Write to BQ completed")
        return len(rows)
  
  