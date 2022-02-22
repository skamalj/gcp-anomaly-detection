from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.window import Timestamp
import apache_beam as beam
from apache_beam import  Pipeline, io, ParDo
import logging, argparse
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions
from packages import WriteToYCQL, SaveAndTimestampEventsBatch, WriteToYSQL
from packages import  CountAndCollectEvents, TrainModels, LoadModels, CalculateDistances
from packages import  LogEventsWithTimestampAndWindow
from apache_beam.utils.timestamp import MAX_TIMESTAMP

FORMAT = '%(asctime)s %(module)s %(funcName)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(input_sub, model_load_timer, pipeline_args=None):
    import random
    # Set `save_main_session` to True so DoFns can access globally imported modules.

    pipeline_options_stream = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    stream_pipeline = Pipeline(options=pipeline_options_stream)

    project_id = pipeline_options_stream.view_as(GoogleCloudOptions).project

    raw_events = (stream_pipeline 
                | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{input_sub}"))

    ts_events = raw_events | "Save And Add Timestamp" >> SaveAndTimestampEventsBatch(project_id=project_id)
            
    collected_events = ts_events | "Count And Collect" >> CountAndCollectEvents(project_id=project_id)
    #( collected_events | "Data Read From YCQL" >> beam.combiners.Count.PerKey()
    #    | "Log Data Read from YCQL" >> ParDo(LogEventsWithTimestampAndWindow("Data Read Completed For:")) 
    #)

    trained_models = (collected_events | "Train Models" >> TrainModels(project_id=project_id))
    (trained_models | "Log Trained Models" >> beam.Map(lambda x: logger.info("MODEL TRAINED: {}".format(x))) )
 
    saved_models = (trained_models
        | "Save Models to  YB" >> ParDo(WriteToYCQL(
                table="patient_models",
                project_id=project_id,
                keyspace="pubsub",
                cql_port=9042,
                cql_user_secret_name="cql_user",
                cql_password_secret_name="cql_password",
                cql_host_secret_name="cql_host",
                cql_ca_cert_secret_name="cql_ca_cert",
                insert_stmt="INSERT INTO pubsub.patient_models (id, model) VALUES (?, ?)"
            )))   
    (saved_models | "Log Saved Models" >> beam.Map(lambda x: logger.info("MODEL SAVED: {}".format(x))) )

    first_timestamp = Timestamp.now()
    model_trigger = (stream_pipeline 
                | 'PeriodicImpulse' >> PeriodicImpulse(first_timestamp,MAX_TIMESTAMP, model_load_timer, False))
    model_input = model_trigger | "Load Saved Models" >> LoadModels(project_id=project_id)

    
    norm_distances = ts_events | "Calculate Distances" >> CalculateDistances(project_id=project_id, model_input=model_input)
    #norm_distances | "Log Norm Distances" >> ParDo(LogEventsWithTimestampAndWindow("Norm Distances: "))

    is_anomaly = norm_distances | "Filter Anomalies" >> beam.Filter(lambda x: x[1] > 3.0)
    is_anomaly | "Log Anomalies" >> ParDo(LogEventsWithTimestampAndWindow("Anomaly Detected: "))

    #(is_anomaly
    #    | "Batch Together For Saving Anomaly Alert" >> beam.GroupBy(lambda x: 1)
    #    | "Filter empty" >> beam.Filter(lambda x: len(x) > 0)
    #    | "Save Alerts to YSQL" >> ParDo(WriteToYSQL(
    #            table="alerts",
    #            database="yugabyte",
    #            project_id=project_id,
    #            port=5433,
    #            user_secret_name="cql_user",
    #            password_secret_name="cql_password",
    #            host_secret_name="cql_host",
    #            ca_cert_secret_name="cql_ca_cert"
    #        )))   

    stream_pipeline.run()


if __name__ == "__main__":
      

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_sub",
        help="The Cloud Pub/Sub subscription to read from.", default="sourcesub"
    )
    parser.add_argument(
        "--model_load_timer",
        help="TPulse time for model loads", default=120
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_sub,
        known_args.model_load_timer,
        pipeline_args,
    )