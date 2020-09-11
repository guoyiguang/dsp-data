import boto3
from  datetime import datetime,timezone,timedelta

def mask_partitions(time_slot):
    bucket_name = 'dsp-service'
    client = boto3.client('athena',region_name='cn-north-1')
    config = {
        'OutputLocation': 's3://' + bucket_name + '/athena_output/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    sql = "alter table sbs_imp add  if not exists partition(pdate='%s') LOCATION 's3://dsp-service/realtime_data/sbs_imp/success/pdate=%s'"%(time_slot,time_slot)

    context = {'Database': 'realtime'}

    client.start_query_execution(QueryString=sql,
                                 QueryExecutionContext=context,
                                 ResultConfiguration=config)


t = datetime.now(timezone(timedelta(hours=8)))
time_slot=(t - timedelta(hours=1)).strftime("%Y%m%d%H")
mask_partitions(time_slot)
