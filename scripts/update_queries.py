import re
import requests
import json

data = json.dumps({"operationName":"ListQueries","variables":{"favs_last_24h":False,"favs_last_7d":True,"favs_last_30d":False,"favs_all_time":False,"author_name":"balancerlabs","limit":1000,"offset":0,"order":[{"query_favorite_count_last_7d":{"favorite_count":"desc_nulls_last"}},{"id":"asc"}],"is_archived":False},"query":"query ListQueries($session_id: Int, $author_name: String, $tags: jsonb_comparison_exp, $query: String_comparison_exp, $is_archived: Boolean!, $limit: Int!, $offset: Int!, $order: [queries_order_by!], $favs_last_24h: Boolean! = false, $favs_last_7d: Boolean! = false, $favs_last_30d: Boolean! = false, $favs_all_time: Boolean! = false) {\n  queries(\n    where: {user: {name: {_eq: $author_name}}, is_archived: {_eq: $is_archived}, is_temp: {_eq: false}, tags: $tags, name: $query}\n    limit: $limit\n    offset: $offset\n    order_by: $order\n  ) {\n    ...Query\n    favorite_queries(where: {user_id: {_eq: $session_id}}, limit: 1) {\n      created_at\n      __typename\n    }\n    __typename\n  }\n  queries_aggregate(\n    where: {user: {name: {_eq: $author_name}}, is_archived: {_eq: $is_archived}, is_temp: {_eq: false}, tags: $tags, name: $query}\n  ) {\n    aggregate {\n      count\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment Query on queries {\n  id\n  dataset_id\n  name\n  description\n  query\n  private_to_group_id\n  is_temp\n  is_archived\n  created_at\n  updated_at\n  schedule\n  tags\n  parameters\n  user {\n    ...User\n    __typename\n  }\n  visualizations {\n    id\n    type\n    name\n    options\n    created_at\n    __typename\n  }\n  favorite_queries_aggregate @include(if: $favs_all_time) {\n    aggregate {\n      count\n      __typename\n    }\n    __typename\n  }\n  query_favorite_count_last_24h @include(if: $favs_last_24h) {\n    favorite_count\n    __typename\n  }\n  query_favorite_count_last_7d @include(if: $favs_last_7d) {\n    favorite_count\n    __typename\n  }\n  query_favorite_count_last_30d @include(if: $favs_last_30d) {\n    favorite_count\n    __typename\n  }\n  __typename\n}\n\nfragment User on users {\n  id\n  name\n  profile_image_url\n  __typename\n}\n"})
headers = {'x-hasura-api-key': ''}

response = requests.post('https://core-hsr.duneanalytics.com/v1/graphql', data=data, headers=headers)
queries = response.json()['data']['queries']

for query in queries:
    uid = str(query['id'])
    name = query['name'].lower().replace('balancer', '').strip()
    name = re.sub(r'[.,()%]', '', name)
    name = re.sub(r'[\s/-]', '_', name)
    filename = uid + '_' + name
    sql_code = query['query']

    with open(f'./queries/{filename}.sql', 'w') as file:
        file.write(sql_code)
    file.close()
