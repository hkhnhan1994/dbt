"""Generate datalake model.yaml from existing tables."""

from typing import List
from google.cloud import bigquery
import yaml
import pathlib
from jinja2 import Template, DebugUndefined

def BQ_schema_to_yml_dict(domain,table_name,tags,schema: List[bigquery.SchemaField],ver='current'):
    """
    Transform a list of SchemaFields into a list of dictionaries.

    :param schema: the schema of the table as a list of SchemaFields
    :return: the schema of the table in dictionary format
    """
    schema_fields = [{'name': 'INSERT_HIST_TIMESTAMP'}]
    for entry in schema:
        schema_fields.append({"name":entry.name})
    return {
        'models':[
            {
            'name':f'dl_{domain}_{table_name}',
            'description': f'auto render table {table_name}',
            'config':{
                'materialized': 'table',
                'dataset':f'dl_{domain}',
                'alias': f"{table_name}",
                'tags': tags,
            },
            'columns': schema_fields
        }
        ]
    } 

def generate_template(domain,table_name, ver = "current", pk_columns = [], ):
    if ver == "hist":

        query ="""
WITH dedup_table AS (

SELECT * EXCEPT(rn)
FROM (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY ingestion_meta_data_uuid ORDER BY ingestion_meta_data_processing_timestamp) AS rn
    FROM  {% raw %} {{ {% endraw %} source('{{ domain }}', '{{ table_name }}') {% raw %} }} {% endraw %}
)
WHERE rn = 1
),
surrogate_key as(
    select
        {% raw %} {{ {% endraw %} dbt_utils.generate_surrogate_key([
                'ingestion_meta_data_uuid', 
                'row_hash'
            ])
        {% raw %} }} {% endraw %} as T_UNIQUE_KEY, 
    CURRENT_TIMESTAMP AS INSERT_HIST_TIMESTAMP,
        *
    FROM dedup_table
)
SELECT 
    *
FROM surrogate_key
        """ 
    else:
        query ="""
WITH current_table AS (
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY _pk_id ORDER BY INSERT_HIST_TIMESTAMP DESC) AS rn
    FROM (
        SELECT
        CONCAT( {%- for col_pk in pk_columns %} `{{col_pk}}` {% if not loop.last %} , {% endif %} {%- endfor %} ,"") AS _pk_id,
        *
        FROM {% raw %} {{ {% endraw %} ref('dl_{{ domain }}_{{ table_name }}') {% raw %} }} {% endraw %}
    ) 
)
SELECT * EXCEPT(rn)
FROM current_table
WHERE rn = 1
        """
    _query =  Template(undefined=DebugUndefined, 
                            source=query,
                            )
    return _query.render(
        domain = domain,
        table_name= table_name,
        pk_columns = pk_columns,
    )

env ="dev"
project_id = "pj-bu-dw-data-sbx"
client = bigquery.Client(project=project_id)

whitelist_path = '/home/hkhnhan/Code/dbt/dbt/models/STRH/datalake/dl_eID_source.yml'
tags = ['datalake']
with open(pathlib.Path(whitelist_path),'r') as f:
    sources = yaml.safe_load(f)
for source in sources['sources']:
    parent_file_name = source['name']
    dataset =  source['schema'].replace('{{ target.schema }}',env).strip()
    for table in source['tables']:
        table_name = table['name']
        pk_id = table['pk_id']
        save_path = f'dl/{parent_file_name}/{table_name}/dl_{parent_file_name}_{table_name}_current.sql'
        content= generate_template(parent_file_name,table_name,"current",pk_id)
        output_file = pathlib.Path(save_path)
        output_file.parent.mkdir(exist_ok=True, parents=True)
        with open(save_path, "w") as fw:
            fw.write(content)
        
        save_path = f'dl/{parent_file_name}/{table_name}/dl_{parent_file_name}_{table_name}.sql'
        content= generate_template(parent_file_name,table_name,"hist",pk_id)
        with open(save_path, "w") as fw:
            fw.write(content)

        bq_table = client.get_table(f"{project_id}.{dataset}.{table_name}")
        save_path = f'dl/{parent_file_name}/{table_name}/dl_{parent_file_name}_{table_name}.yml'
        cur_schema = BQ_schema_to_yml_dict(
            domain=f"{parent_file_name}",
            table_name=table_name,
            tags = tags,
            schema = bq_table.schema,
            ver="hist"
            )
        with open(save_path, "w") as fw:
            yaml.dump(cur_schema, fw, default_flow_style=False,sort_keys=False)

        save_path = f'dl/{parent_file_name}/{table_name}/dl_{parent_file_name}_{table_name}_current.yml'
        cur_schema = BQ_schema_to_yml_dict(
            domain=f"{parent_file_name}",
            table_name=f'{table_name}_current',
            tags = tags,
            schema = bq_table.schema,
            ver="current"
            )
        with open(save_path, "w") as fw:
            yaml.dump(cur_schema, fw, default_flow_style=False,sort_keys=False)