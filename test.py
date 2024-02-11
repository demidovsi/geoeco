from google.cloud import bigquery

# Create a client object
client = bigquery.Client()

# Define your query
query = """
    with russia_Data as (
SELECT distinct 
  id.country_name,
  id.value, --format in DataStudio
  id.indicator_name
FROM (
  SELECT
    country_code,
    region
  FROM
    bigquery-public-data.world_bank_intl_debt.country_summary
  WHERE
    region != "" ) cs --aggregated countries do not have a region
INNER JOIN (
  SELECT
    country_code,
    country_name,
    value, 
    indicator_name
 

  FROM
    bigquery-public-data.world_bank_intl_debt.international_debt
  WHERE true
    and country_code = 'RUS'
    
     ) id
ON
  cs.country_code = id.country_code
  where value  is not null
ORDER BY
  id.value DESC
)
select * 
from russia_data
"""

st = 'SELECT * FROM bigquery-public-data.world_bank_intl_debt.country_summary'
# Execute the query
query_job = client.query(query)

# Fetch the results
results = query_job.result()

# Print the results
for row in results:
    print(row)