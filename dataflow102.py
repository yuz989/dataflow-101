import logging
import const
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def to_count(val):
    return {
        "count": val['total']
    }


def run():
    query = '''SELECT COUNT(*) as total FROM [enhanced-optics-170711:db.user_tbl]'''
    TABLE = 'enhanced-optics-170711:stats_db.userCount_tbl'
    TABLE_SCHEMA = ('count:INTEGER')

    pipeline_options = PipelineOptions(
        project=const.PROJECT,
        staging_location=const.STAGING_LOCATION,
        temp_location=const.TEMP_LOCATION,
        job_name='dataflow102',
        runner='DataflowRunner',
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        pipeline | beam.io.Read(
            beam.io.BigQuerySource(
                query=query,
            ))\
            | beam.Map(to_count)\
            | beam.io.Write(beam.io.BigQuerySink(
                table=TABLE,
                schema=TABLE_SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            ))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()