import requests

http_url = "http://localhost:8080/graphql"

def query(query, variables=None, headers=None):
    payload = {'headers': headers, 'query': query, 'variables': variables}
    resp = requests.post(http_url, json=payload)
    return resp


def mutation(query, variables=None, headers=None):
    payload = {'headers': headers, 'query': query, 'variables': variables}
    resp = requests.post(http_url, json=payload)
    return resp
