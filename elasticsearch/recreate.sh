# recreating index

echo "recreating index"
curl -XDELETE "http://localhost:9200/users"
curl -XPUT "localhost:9200/users"
curl -XPUT "http://localhost:9200/users/_mapping/user" -d @_mapping_user.json

