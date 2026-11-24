# %%
import sys
!{sys.executable} -m pip install google-cloud-bigquery

# %%
from google.cloud import bigquery

# upload to BigQuery
client = bigquery.Client(project="deassignment2-515613")   # use your project id

table_ref = client.dataset("assignment2").table("frequency")  # use the correct dataset name and table name

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1 # ignore the header
job_config.autodetect = True

with open("/home/jovyan/data/usage_frequency.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file, table_ref, job_config=job_config
    )

# job is a sync operation so we have to wait for it to finish
job.result()

# %%
# Perform a query.
QUERY = (
    'SELECT * FROM `deassignment2-515613.assignment2.frequency` LIMIT 100')   # use the correct project id, etc.
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row)


