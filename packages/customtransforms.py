import apache_beam as beam
from apache_beam import ParDo,  CombinePerKey
from apache_beam.transforms import window, trigger
from apache_beam.transforms.trigger import AfterProcessingTime, AfterWatermark
from apache_beam.transforms.ptransform import ptransform_fn
from packages import AddTimestampDoFn, StatefulCounterDoFn, ReadModelsFromYCQL, parse_simulated_events
from packages import ReadFromYCQL, WriteToBQ, LogEventsWithTimestampAndWindow, WriteToYCQLBatch
from packages import  create_kv_events, train_model, is_anomolous, update_distance, element_from_kv


@ptransform_fn
def SaveAndTimestampEventsBatch(raw_events, project_id):
        ts_events = (raw_events
                | "Create Simulated Events" >> beam.Map(parse_simulated_events)
                | "Create KV" >> beam.Map(create_kv_events)
                | "Add Timestamp" >> ParDo(AddTimestampDoFn())
        )
        (ts_events 
                | "Apply Window" >> beam.WindowInto(window.FixedWindows(2),trigger=AfterWatermark(
                                early=AfterProcessingTime(delay=1), late=AfterProcessingTime(delay=1)), 
                                allowed_lateness=2, accumulation_mode=trigger.AccumulationMode.DISCARDING)
                | "Batch Together Raw Events For Insert" >> beam.GroupBy(lambda x: 1)
                | "Write To YB" >> ParDo(WriteToYCQLBatch(
                table="patient_raw_data",
                project_id=project_id,
                keyspace="pubsub",
                cql_port=9042,
                cql_user_secret_name="cql_user",
                cql_password_secret_name="cql_password",
                cql_host_secret_name="cql_host",
                cql_ca_cert_secret_name="cql_ca_cert",
                event_decoder=element_from_kv,
                insert_stmt="UPDATE pubsub.patient_raw_data SET value = ? WHERE device_id = ? AND  eventtime = ?"
            )))

        return ts_events


@ptransform_fn
def CountAndCollectEvents(ts_events, project_id):
        collected_events = (ts_events
                | "Count Events" >> ParDo(StatefulCounterDoFn(limit_count=100))
        )
        (collected_events | "Log Data Collection" >> ParDo(LogEventsWithTimestampAndWindow("Training Events Collected For :")))
        training_data = (collected_events
                | "Apply Window" >> beam.WindowInto(window.FixedWindows(15), 
                                trigger=trigger.AfterProcessingTime(20), 
                                allowed_lateness=30, accumulation_mode=trigger.AccumulationMode.DISCARDING)
                | "Read Training Data" >> ParDo(ReadFromYCQL(
                    table="patient_raw_data",
                    project_id=project_id,
                    keyspace="pubsub",
                    cql_port=9042,
                    cql_user_secret_name="cql_user",
                    cql_password_secret_name="cql_password",
                    cql_host_secret_name="cql_host",
                    cql_ca_cert_secret_name="cql_ca_cert",
                    limit=300 )))
        return training_data

@ptransform_fn
def TrainModels(collected_events, project_id):
        trained_models = ( collected_events
                    | "Load To BQ" >> CombinePerKey(WriteToBQ(project_id=project_id,dataset_id="pubsub",table_id="patient_raw_data"))
                    | "Create Load Batches" >> beam.Map(lambda x: (x[0][0], x[1]))
                    | "Await Batch Load" >> beam.GroupByKey() 
                    | "Train Model" >> beam.Map(train_model,limit=300)
                )
        return trained_models

@ptransform_fn
def LoadModels(model_trigger, project_id):
        model_input = ( model_trigger
                | "Load Saved Models" >> beam.ParDo(ReadModelsFromYCQL(
                    table="patient_models",
                    project_id=project_id,
                    keyspace="pubsub",
                    cql_port=9042,
                    cql_user_secret_name="cql_user",
                    cql_password_secret_name="cql_password",
                    cql_host_secret_name="cql_host",
                    cql_ca_cert_secret_name="cql_ca_cert"
                ))
                | "Window by Timestamp" >> beam.WindowInto(window.FixedWindows(120),allowed_lateness=120,
                    trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                    accumulation_mode=trigger.AccumulationMode.DISCARDING
                )
        )
        return model_input

@ptransform_fn
def CalculateDistances(ts_events, project_id, model_input):
        norm_distances = ( ts_events
                | "Stream For Inference" >> beam.WindowInto(window.FixedWindows(2),trigger=AfterWatermark(
                                early=AfterProcessingTime(delay=1), late=AfterProcessingTime(delay=1)), 
                                allowed_lateness=120, accumulation_mode=trigger.AccumulationMode.DISCARDING)
                | 'Calculate Distance' >> beam.Map(is_anomolous, models=beam.pvalue.AsSingleton(model_input))
        )
        (norm_distances 
                | "Batch Together Raw Events For Insert" >> beam.GroupBy(lambda x: 1)
                | "Write To YB" >> ParDo(WriteToYCQLBatch(
                table="patient_raw_data",
                project_id=project_id,
                keyspace="pubsub",
                cql_port=9042,
                cql_user_secret_name="cql_user",
                cql_password_secret_name="cql_password",
                cql_host_secret_name="cql_host",
                cql_ca_cert_secret_name="cql_ca_cert",
                event_decoder=update_distance,
                insert_stmt="UPDATE pubsub.patient_raw_data SET distance = ? WHERE device_id = ? AND  eventtime = ?"
            )))
        return norm_distances
