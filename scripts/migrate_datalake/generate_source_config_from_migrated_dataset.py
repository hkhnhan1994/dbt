from dataset_migrate import datalake_config
import yaml


prefix_db = "public_"
whitelist =["h3_hehe", "h3_hklc", "h1_hkvk"]

class LiteralStr(str):
    pass
def literal_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
yaml.add_representer(LiteralStr, literal_representer)
sources = []
for dataset, tables in datalake_config.items():
    db = {}
    if dataset.lower() in whitelist:
        db["name"] = dataset.lower()
        db["schema"] = LiteralStr('{{ target.schema }}_lake_view_cmd\n')
        db["tables"] = []
        for tb in tables:
            table = {}
            table["name"] = prefix_db + tb.get('table_name')
            if isinstance(tb.get('primary_key'),list):
                table["pk_id"] = tb.get('primary_key')
            else: table["pk_id"] = [tb.get('primary_key')]
            if isinstance(tb.get('update_timestamp'),list):
                table["update_timestamp"] = tb.get('update_timestamp')
            else: 
                if tb.get('update_timestamp'):
                    table["update_timestamp"] = [tb.get('update_timestamp')]
            db["tables"].append(table)
        sources.append(db)

result = {
"sources": sources
}

save_path = f'dl/config_source.yml'
with open(save_path, "w") as fw:
    yaml.dump(result, fw, default_flow_style=False,sort_keys=False,allow_unicode=True)