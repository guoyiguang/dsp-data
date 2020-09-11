import boto3
import json
import time
import pandas as pd
import pymysql
import sys
import datetime
from sqlalchemy import create_engine


# 获取前两个小时 所有数据 来补充前2个小时的数据 防止数据缺失
def get_sql(t2):
    sql = '''
        SELECT
        COALESCE(click.campaign,imp.campaign) as campaign,
        COALESCE(click.time_slot,imp.time_slot) as time_slot,
        COALESCE(click.platform,imp.platform) as platform,
        COALESCE(click.slot_id,imp.slot_id) as slot_id,
        COALESCE(click.ad,imp.ad) as ad,
        COALESCE(click.user_id,imp.user_id) as user_id,
        COALESCE(click.adx,imp.adx) as adx,
        COALESCE(click.media_id,imp.media_id) as media_id,
        imp_num as impression,
        click_num as click,
        revenue
FROM
        ( SELECT campaign, time_slot, platform, slot_id, ad, user_id,adx, media_id, sum( count ) AS imp_num FROM impression where pdate >= '%s'  group by campaign, user_id,time_slot, platform, slot_id, ad, adx, media_id ) imp
        full JOIN ( SELECT campaign, time_slot, platform, slot_id, user_id,ad, adx, media_id, sum( count ) AS click_num, sum(price*count) as revenue FROM click where status='1' and  pdate >= '%s' group by campaign, user_id,time_slot, platform, slot_id, ad, adx, media_id ) click 
        on click.campaign = imp.campaign 
        AND click.platform = imp.platform 
        AND click.slot_id = imp.slot_id 
        AND click.ad = imp.ad
        AND click.time_slot = imp.time_slot
        AND click.adx = imp.adx
        AND click.media_id = imp.media_id
        AND click.user_id = imp.user_id

    ''' % (t2, t2)
    return sql

# 获取自建站的点击作为dsp的转化
def get_sbs_click(t2):
    sbs_sql = '''
        select user_id,campaign, time_slot,case platform when '2' then 'Android'  else 'iOS' end as platform ,slot_id,ad_id as ad,adx,count(distinct(imp_id)) as conversion from sbs_imp where time_slot >= '%s' and e = '107' group by user_id,campaign,time_slot,platform,slot_id,ad_id,adx
        ''' % (t2)
    return sbs_sql

def from_athena_data(sql):
    bucket_name = 'dsp-service'
    client = boto3.client('athena', region_name='cn-north-1')
    config = {
        'OutputLocation': 's3://' + bucket_name + '/athena_output/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    context = {'Database': 'realtime'}
    response = client.start_query_execution(QueryString=sql,
                                            QueryExecutionContext=context,
                                            ResultConfiguration=config)
    while True:
        stats = client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        status = stats['QueryExecution']['Status']['State']
        #            print(term.yellow("Stats: "+ status))

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if status == 'SUCCEEDED':
        rows = []
        cleanrows = []
        paginator = client.get_paginator('get_query_results')
        response_iterator = paginator.paginate(
            QueryExecutionId=response['QueryExecutionId'],
            PaginationConfig={
                'PageSize': 1000,
            }
        )
        for results in response_iterator:
            rows.extend(results['ResultSet']['Rows'])
        rows = [r['Data'] for r in rows]
        for r in rows:
            line = []
            for i in r:
                try:
                    line.append(i['VarCharValue'])
                except:
                    line.append('')
            cleanrows.append(line)

        # 将第一行提取出来作为 columns
        columns = cleanrows.pop(0)
        df = pd.DataFrame(cleanrows, columns=columns)

        return df
    else:
        print("invalid msg")


mysql_config = {
    'host': 'primedsp.ckaigoijxdbf.rds.cn-north-1.amazonaws.com.cn',
    'user': 'user',
    'password': 'pass',
    'database': 'dsp',
    'port': 3306

}

db = pymysql.connect(**mysql_config)
cursor = db.cursor()
time_slot = time.strftime("%Y%m%d%H", time.localtime())
def athean_join_campaign(athena_df,sbs_click_df):

        sql="select id as campaign,landing_page_id from ad_campaign where landing_page_id is not null and landing_page_id !=''"
        campaign_df = pd.read_sql_query(sql, db)
        print("*"*30)

        campaign_df=campaign_df.astype(object)
        athena_df['campaign'] =athena_df['campaign'].astype(int)
        sbs_click_df['campaign'] = sbs_click_df['campaign'].astype(int)

        print("athena_df 的类型")
        print(athena_df.dtypes)
        print("sbs_click_df 的类型")
        print(sbs_click_df.dtypes)
        athena_join_campaign_df = pd.merge(athena_df, campaign_df, how='left', on='campaign')
        athena_join_sbs_df = pd.merge(athena_join_campaign_df,sbs_click_df,how='left')
        return athena_join_sbs_df
def write_to_mysql(t2, df):
    try:
        # 删除 前两个小时的数据
        sql = "delete from  dsp_base_table where time_slot >= %s" % t2
        cursor.execute(sql)
        db.commit()
        db.close()
        cursor.close()
        conn = create_engine(
            'mysql+pymysql://user:password@primedsp.ckaigoijxdbf.rds.cn-north-1.amazonaws.com.cn:3306/dsp?charset=utf8')

        pd.io.sql.to_sql(df, 'dsp_base_table', conn, schema='dsp', if_exists='append', index=False)

    except  Exception as e:
        print("invalid msg:", e.args, file=sys.stderr)


if __name__ == '__main__':
    import time

    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    time.sleep(2)
    t = datetime.datetime.now()
    # 获取上个小时

    t1 = (t - datetime.timedelta(hours=1)).strftime("%Y%m%d%H")
    # 获取前两个小时
    t2 = (t - datetime.timedelta(hours=3)).strftime("%Y%m%d%H")
    print(t, t2)

    campaign_sql = get_sql(t2)
    sbs_click_sql = get_sbs_click(t2)

    athena_df = from_athena_data(campaign_sql)
    sbs_click_df = from_athena_data(sbs_click_sql)


    athean_join_campaign_df = athean_join_campaign(athena_df,sbs_click_df)

    write_to_mysql(t2, athean_join_campaign_df)