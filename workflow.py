
from dbt_workflows_factory import dbt_workflows_converter, params
params = params(image_uri='my_image_url', region='europe-west1', full_command='dbt run', remote_path='/mnt/disks/var', key_colume_mount_path='/mnt/disks/var/keyfile_name.json', key_volume_path='/mnt/disks/var/:/mnt/disks/var/:rw', key_path='bucketname' )

converter = dbt_workflows_converter(params)
converter.convert() # writes to file workflow.yaml