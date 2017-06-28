import logging
import time
import datetime
import random
import apache_beam as beam
import const
from apache_beam.options.pipeline_options import PipelineOptions


def gen_random_user(id):
    return {
        "name_col": "name" + str(id),
        "email_col": "email" + str(id) + "@mail.com",
        "address_col": "address" + str(id),
        "age_col": random.randint(1, 100),
        "password_col": ''.join(random.choice('abcdefghijABCDEFGHIJ1234567890') for _ in range(32)),
        "createTime_col": datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    }

def run():
    TABLE = 'enhanced-optics-170711:db.user_tbl'
    TABLE_SCHEMA = ('name_col:STRING, email_col:STRING, address_col:STRING, age_col:INTEGER,'
                    'password_col:STRING, createTime_col:DATETIME')

    pipeline_options = PipelineOptions(
        project=const.PROJECT,
        staging_location=const.STAGING_LOCATION,
        temp_location=const.TEMP_LOCATION,
        job_name='dataflow101',
        runner='DataflowRunner',
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:

        pipeline | beam.Create([random.randint(1, 1000) for _ in range(0, 100)])\
            | beam.Map(gen_random_user)\
            | beam.io.Write(beam.io.BigQuerySink(
                table=TABLE,
                schema=TABLE_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
              ))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
