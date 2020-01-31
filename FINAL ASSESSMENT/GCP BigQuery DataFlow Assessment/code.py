import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText

def run(argv=None):

    p = beam.Pipeline(options=PipelineOptions())


    class Printer(beam.DoFn):
        def process(self, element):
            print element

    class Transaction(beam.DoFn):
        def process(self, element):
            t=[]
            t=element.split(',')
            print(len(t))
            if t[0]!='seasonType':
                return [{"seasonType": t[0],"year": t[1],"attendance" : t[2],"dayNight" : t[3],"durationMinutes" : int(t[4])*2,"awayTeamName" : t[5],"homeTeamName" : t[6][::-1],"venueName" : t[7],"venueCapacity" : t[8],"venueCity" : t[9],"venueZip": t[10]}]


    data_from_source = (p
                        | 'Read Source' >> ReadFromText('gs://gcp-geeks-01-bucket/games_post_wide.csv')
                        | 'Transform' >> beam.ParDo(Transaction())
                        )

    project_id = "pe-training"
    dataset_id = 'gcp_geeks_01_baseball'
    table_id = 'test'
    table_schema = ('seasonType:STRING,year:INTEGER,attendance:INTEGER,dayNight:STRING,durationMinutes:INTEGER,awayTeamName:STRING,homeTeamName:STRING,venueName:STRING,venueCapacity:INTEGER,venueCity:STRING,venueZip:STRING')

    data_from_source | 'Load' >> beam.io.WriteToBigQuery(
                    table=table_id,
                    dataset=dataset_id,
                    project=project_id,
                    schema=table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=int(100)
                    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    run()




