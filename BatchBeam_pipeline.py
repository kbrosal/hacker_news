import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import datetime
from google.cloud import bigquery

#Creating an Argumentparser object to input data file.
parser = argparse.ArgumentParser()

parser.add_argument(
    '--input',
    dest = 'input',
    required = True,
    help = "Path to the input file to be processed."
)

#Parsing known arguments from the CLI.
path_args, pipeline_args = parser.parse_known_args()

file_path = path_args.input

pipeline_options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options = pipeline_options)

#Transformation and Table Creation

def filter_year(row):
    columns = row.split(',')
    date_str = columns[6]

    try:
        date_obj = datetime.datetime.strptime(date_str, '%m/%d/%Y %H:%M')
        if 2014 <= date_obj.year <= 2016:
            columns[6] = date_obj.strftime('%Y-%m-%d %H:%M:00')
            return ','.join(columns)
    except ValueError:
        return None


dataset_hn = (
    p
    | 'Read CSV file' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
    | 'Filter by Year' >> beam.Filter(lambda row: filter_year(row) is not None)
)

ask_HN = (
    dataset_hn
    | 'posts to ask HN' >> beam.Filter(lambda row: 'Ask HN:' in row.split(',')[1])

)

show_HN = (
    dataset_hn
    |'posts to show HN' >> beam.Filter(lambda row: 'Show HN:' in row.split(',')[1])

)

def print_row(row):
    print(row)

(dataset_hn
 | 'count mapping' >> beam.combiners.Count.Globally()
 | 'total mapping' >> beam.Map(lambda x: 'Total Count:' +str(x))
 | 'print total' >> beam.Map(print_row)
)

(ask_HN
 | 'count ask_HN' >> beam.combiners.Count.Globally()
 | 'ask_HN total mapping' >> beam.Map(lambda x: 'Total Count:' +str(x))
 | 'print ask_HN total' >> beam.Map(print_row)
)

(show_HN
 | 'count show_HN' >> beam.combiners.Count.Globally()
 | 'show_HN total mapping' >> beam.Map(lambda x: 'Total Count:' +str(x))
 | 'print show_HN total' >> beam.Map(print_row)
)

#Creating dataset and Loading Data to Biqquery

client = bigquery.Client()

dataset_id = "{}.hacker_news".format(client.project)

try:
    client.get_dataset(dataset_id)

except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "asia-southeast1"
    dataset.description = "Dataset of hacker news showing ask and show HN"

    dataset_ref = client.create_dataset(dataset, timeout=30) # Making and API request to BQ


def to_json(csv_str):
    fields = csv_str.split(',')

    if len(fields) != 7: 
        return None
    try:

        json_str = {"id":int(fields[0]),
                    "title": fields[1],
                    "url": fields[2],
                    "num_points": int(fields[3]),
                    "num_comments": int(fields[4]),
                    "author": fields[5],
                    "created_at": datetime.datetime.strptime(fields[6],'%m/%d/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
                    }

        return json_str
    except ValueError as e:
        print(f"Error processing row: {csv_str} - {e}")
        return None

table_schema = 'id:INTEGER, title:STRING, url:STRING, num_points:INTEGER, num_comments:INTEGER, author:STRING, created_at:TIMESTAMP'

#project-id:dataset_id.table_id
askHN_table_spec = 'data-engineering-433013:hacker_news.ask_HN'
#project-id:dataset_id.table_id
showHN_table_spec = 'data-engineering-433013:hacker_news.show_HN'

(ask_HN
 | 'ask HN to json' >> beam.Map(to_json)
 | 'write ask HN' >> beam.io.WriteToBigQuery(
     askHN_table_spec,
     schema = table_schema,
     create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
     additional_bq_parameters = {'timePartitioning': {'type': 'DAY'}},
     custom_gcs_temp_location = "gs://beam_hnews"
 )

)

(show_HN
 | 'show HN to json' >> beam.Map(to_json)
 | 'write show HN' >> beam.io.WriteToBigQuery(
     showHN_table_spec,
     schema = table_schema,
     create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
     additional_bq_parameters = {'timePartitioning': {'type': 'DAY'}},
     custom_gcs_temp_location = "gs://beam_hnews"
 )

)

from apache_beam.runners.runner import PipelineState
ret = p.run()
if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error in Running Beam pipeline') 

# Creating View Table
view_name = "hackerNews_viewTable"
dataset_ref = client.dataset('hacker_news')
view_ref = dataset_ref.table(view_name)
view_to_create = bigquery.Table(view_ref)

view_to_create.view_query = 'SELECT * FROM `data-engineering-433013.hacker_news.ask_HN` WHERE _PARTITIONDATE = DATE(current_date())'
view_to_create.view_use_legacy_sql = False

try:
    client.create_table(view_to_create)
except:
    print("View already exist")
