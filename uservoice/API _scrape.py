import requests
import pandas as pd
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from time import sleep
from numpy import array_split
import csv
from datetime import datetime

API_KEY = 'enter your api key'
API_SECRET = 'enter you api key secret'
URL = "domain for babbel user voice"


def get_auth_token(api_key, api_secret, url):
    client = BackendApplicationClient(client_id=api_key)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=url, client_id=api_key,
            client_secret=api_secret)
    return token


t = get_auth_token(api_key=API_KEY, api_secret=API_SECRET, url=URL)

default_headers = { 
    'Content-Type': 'application/json', 
    'Accept': 'application/json',  
    'API-Client': 'python' ,
    'Authorization': f'Bearer {t["access_token"]}'
}

def unpack_request_data(data):
    req_data = {}
    req_data['suggestion_id'] = data['links']['suggestion']
    req_data['user_id'] = data['links']['user'] # not our user_id
    req_data['request'] = data['body']
    req_data['source'] = data['source_url']
    req_data['request_id'] = data['id']
    req_data['created_at'] = data['created_at']
    return req_data


def request_requests(headers, page_number, number_of_records):
    
    payload = {
        'page': page_number, 
        'per_page': number_of_records, 
        'sort': '-updated_at', 
        'source': 'zendesk_ticket'
    }
    suggestions_url = 'https://babbel.uservoice.com/api/v2/admin/requests'
    response = requests.get(suggestions_url, headers=headers, params=payload)
    status = response.status_code
    if status == 200:
        data = response.json()
        try:
            return {'request_data': [unpack_request_data(x) for x in data['requests']], 
                    'pagination': data['pagination']}
        except KeyError as exc:
            # the message contains a warning about the api rate limit
            if list(data.keys())[0] == 'message':
                return 'rate limited'
    elif status == 429:
        # this error code has come up once
        print('too many requests error')
        return 'rate limited'
    else:
        raise Exception(f'status code: {status} returned')

## TODO: make function out of this
## TODO: better api request limit handling
## loop through request pages
requests_data = []
page = 1
## TODO: restore while loop

#for i in range(5):
while True:
    data = request_requests(headers=default_headers, page_number=page, number_of_records=60)
    if data == 'rate limited':
        print("request has been rate limited")
        print('sleeping 10 seconds')
        sleep(10)
        continue
    total_pages = data['pagination']['total_pages']
    ## TODO: validate the data here
    requests_data.extend(data['request_data'])
    print(f'page {page} collected')
    page += 1
    if page > total_pages:
        print('requests finished')
        break

requests_df = pd.DataFrame(requests_data)


def request_suggestions(suggestion_ids, headers):
    suggestion_ids = [str(x) for x in suggestion_ids]
    suggestion_request_array = ','.join(suggestion_ids)
    suggestions_url = f'https://babbel.uservoice.com/api/v2/admin/suggestions/{suggestion_request_array}'
    response = requests.get(suggestions_url, headers=headers)
    status = response.status_code
    if status == 200:
        data = response.json()
        try:
            return [{'suggestion_title': x['title'], 'suggestion_id': x['id']} for x in data['suggestions']]
        except KeyError as exc:
            # the message contains a warning about the api rate limit
            if list(data.keys())[0] == 'message':
                return 'rate limited'
    elif status == 429:
        # this error code has come up once
        print('too many requests error')
        return 'rate limited'
    else:
        raise Exception(f'status code: {status} returned')


def fetch_suggestions(suggestion_ids):
    """
    pass in unique suggestions
    """
    counter = 0
    print(f'fetching {len(suggestion_ids)} suggestion ids')
    n_chunks = len(suggestion_ids) / 10 
    suggestion_chunks = array_split(suggestion_ids, n_chunks)
    all_suggestion_data = []
    for c in suggestion_chunks:
        # need to retry the chunk if we hit an api limit or 429 error
        for n in range(6): ## number of tries
            suggestion_data = request_suggestions(c, headers=default_headers)
            if suggestion_data == 'rate limited':
                print("request has been rate limited")
                print(f'sleeping 10 seconds on attempt: {n + 1}')
                sleep(10)
                continue
            else:
                assert all([sorted(list(x.keys())) == ['suggestion_id', 'suggestion_title'] \
                            for x in suggestion_data]),\
                        'keys of each dict should be "suggestion_id", "suggestion_title'
                all_suggestion_data.extend(suggestion_data)
                counter += len(c)
                print(f'suggestions collected: {counter}')
                break
    return all_suggestion_data


suggestion_id_list = requests_df.suggestion_id.unique()
suggestion_data = fetch_suggestions(suggestion_id_list)
suggestion_df = pd.DataFrame(suggestion_data)



def request_users(user_ids, headers):
    user_ids = [str(x) for x in user_ids]
    user_id_request_array = ','.join(user_ids)
    user_url = f'https://babbel.uservoice.com/api/v2/admin/users/{user_id_request_array}'
    response = requests.get(user_url, headers=headers)
    status = response.status_code
    if status == 200:
        data = response.json()
        try:
            return [{'user_id': x['id'], 'email': x['email_address']} for x in data['users']]
        except KeyError as exc:
            # the message contains a warning about the api rate limit
            if list(data.keys())[0] == 'message':
                return 'rate limited'
    elif status == 429:
        # this error code has come up once
        print('too many requests error')
        return 'rate limited'
    else:
        raise Exception(f'status code: {status} returned')




def fetch_users(user_ids):
    """
    pass in unique suggestions
    """
    counter = 0
    print(f'fetching {len(user_ids)} user ids')
    n_chunks = len(user_ids) / 20 
    user_chunks = array_split(user_ids, n_chunks)
    all_user_id_data = []
    for user_chunk in user_chunks:
        # need to retry the chunk if we hit an api limit or 429 error
        for n in range(6): ## number of tries
            user_id_data = request_users(user_chunk, headers=default_headers)
            if user_id_data == 'rate limited':
                print("request has been rate limited")
                print(f'sleeping 10 seconds on attempt: {n + 1}')
                sleep(10)
                continue
            else:
                assert all([sorted(list(x.keys())) == ['email', 'user_id'] \
                            for x in user_id_data]),\
                        'keys of each dict should be "email", "user_id'
                all_user_id_data.extend(user_id_data)
                counter += len(user_chunk)
                print(f'users collected: {counter}')
                break
    return all_user_id_data



user_id_list = requests_df.user_id.unique()
user_id_data = fetch_users(user_id_list)
user_id_df = pd.DataFrame(user_id_data)

# write original dfs to csv as well

now = datetime.now()

# losing ids during merge
all_request_data = requests_df.merge(suggestion_df, on='suggestion_id', how='outer')
all_request_data = all_request_data.merge(user_id_df, on='user_id', how='outer')


# final preparation
all_request_data.request = all_request_data.request.apply(lambda x: x.replace('\n', '\\n'))
to_write = all_request_data[['suggestion_title', 'request', 'source', 'email', 'created_at', 'request_id', 'suggestion_id', 'user_id']]


to_write.to_csv(f'request_dump_at_{now:%Y%m%d}.csv', index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')


## data checks

n_requests = requests_df.request_id.nunique()
n_suggestions = suggestion_df.suggestion_id.nunique()
n_users = user_id_df.user_id.nunique()

print(f'n requests in base file: {n_requests}')
print(f'n_suggestions in base file: {n_suggestions}')
print(f'n_users in base file: {n_users}')

n_requests_all = all_request_data.request_id.nunique()
n_suggestions_all = all_request_data.suggestion_id.nunique()
n_users_all = all_request_data.user_id.nunique()

print(f'n requests in merged file: {n_requests_all}')
print(f'n_suggestions in merged file: {n_suggestions_all}')
print(f'n_users in merged file: {n_users_all}')

assert n_requests == n_requests_all, 'request id counts dont match after merge'
assert n_suggestions == n_suggestions_all, 'suggestion id counts dont match after merge'
assert n_users == n_users_all, 'user id counts dont match after merge'


