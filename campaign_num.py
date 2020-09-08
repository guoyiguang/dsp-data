from elasticsearch import Elasticsearch
import datetime
# 链接es
es = Elasticsearch(["X.X.X.X:9200"],http_auth=('elastic','GefHbmd8HpMJaY'))
import pymysql

body={
  "size": 0,
  "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "range": {
            "@timestamp": {
              "gte":"now-1h",
              "lt":"now"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  },
  "aggs": {
    "group_by_field": {
      "terms": {
        "size": 1102,
        "script": "doc['campaign_id'] +'####'+doc['ad_id']+'####'+doc['app_id.keyword']"
      }
    }

  }
}

mysql_config = {
        'host' : "",
        'user' : "",
        'password' : "",
        'database' : "",
        'port' : 3306
        }


result = es.search(index="req-status-2020-09-08",body=body)
result_list = []
for i in result['aggregations']['group_by_field']['buckets']:
    field_list=[]
    key = i['key'].split('####')
    campaign_id = key[0][1:-1]
    ad_id = key[1][1:-1]
    app_id = key[2][1:-1]
    doc_count=i['doc_count']
    time_slot = datetime.datetime.now().strftime('%Y%m%d%H')
    field_list.append(time_slot)
    field_list.append(campaign_id)
    field_list.append(ad_id)
    field_list.append(app_id)
    field_list.append(doc_count)
    result_list.append(field_list)

conn = pymysql.connect(**mysql_config)
cur = conn.cursor()
sql="insert into campaign_num_report(time_slot,campaign_id,ad_id,app_id,count) values(%s,%s,%s,%s,%s)"
try:
  cur.executemany(sql,result_list)
  conn.commit()
except:
  conn.rollback()

conn.close()
