# -*- coding: utf-8 -*-
import json
import os
import urllib2
import sys

index_url = "http://localhost:9200/vk/"    # post request needed

path = "D:\\tmp\\downloading\\"

if len(sys.argv) != 0:
    path = sys.argv[0]

#creating index
opener = urllib2.build_opener(urllib2.HTTPHandler)
request = urllib2.Request('http://localhost:9200/vk')
request.get_method = lambda: 'PUT'
opener.open(request)


#adding type user and mapping
user_mapping = json.loads(open('_mapping_user.json').read().decode('utf-8'))
request = urllib2.Request('http://localhost:9200/vk/_mapping/user',
                          data=unicode(json.dumps(user_mapping, ensure_ascii=False)))
request.get_method = lambda: 'PUT'
opener.open(request)


#adding type post and mapping
post_mapping = json.loads(open('_mapping_post.json').read().decode('utf-8'))
request = urllib2.Request('http://localhost:9200/vk/_mapping/post',
                          data=unicode(json.dumps(post_mapping, ensure_ascii=False)))
request.get_method = lambda: 'PUT'
opener.open(request)


#adding type group and mapping
group_mapping = json.loads(open('_mapping_group.json').read().decode('utf-8'))
request = urllib2.Request('http://localhost:9200/vk/_mapping/group',
                          data=unicode(json.dumps(group_mapping, ensure_ascii=False)))
request.get_method = lambda: 'PUT'
opener.open(request)


files = os.listdir(path)

for file in files:
    s = json.loads(open(file).read().decode('utf-8'))

    user_id = s['id']

    user = {}
    user['id'] = s['id']

    user.update(s['generalInformation'])
    user['groupInformation']['sub_groups_count'] = s['groupInformation']['sub_groups_count']
    user['groupInformation']['groups'] = s['groupInformation']['groups']
    user['groupInformation']['sub_groups_count'] = s['groupInformation']['sub_groups_count']

    user['sub_user_count'] = s['sub_user_count']
    user['followers_count'] = s['followers_count']
    user['wall_count'] = s['wall_count']
    user['albums_count'] = s['albums_count']

    user['friends'] = s['friends']
    user['subscriptions'] = s['subscriptions']
    user['university_name'] = ""

    # add user to index
    urllib2.Request(url=(index_url + "user/"),
                    data=unicode(json.dumps(user, ensure_ascii=False)))

    # add each post to index with specified parent id
    posts = s['wall_content']
    for post in posts:
        urllib2.Request(url=(index_url + "post?parent=" + user_id),
                        data=unicode(json.dumps(post, ensure_ascii=False)))

    # add each group information to index
    groups = s['groupInformation']['groups_detail']
    for group in groups :
        urllib2.Request(url=(index_url + "group?parent=" + user_id),
                        data=unicode(json.dumps(group, ensure_ascii=False)))


