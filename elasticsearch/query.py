import urllib2
import json


index_url = "http://localhost:9200/vk"

doc_type = "/user"

s = json.loads("""{
"query": {
    "has_child": {
        "type": "post",
        "score_mode": "max",
        "query": {
            "query_string" : {
                "default_field" : "text",
                "query" : "Telegram"
            }
        }
    }
}
}""")

opener = urllib2.build_opener(urllib2.HTTPHandler)
request = urllib2.Request(url=(index_url + doc_type + "/_search"),
                          data=json.dumps(s), headers={"Content-Type": "application/json"})
request.get_method = lambda: 'POST'


s = (opener.open(request)).read()
jans = json.loads(s, encoding="utf-8")

hits = jans["hits"]["hits"]
for hit in hits:
    print hit["_source"]

