"""

"""
import requests
import json

from utils import helper_functions


def make_contract_request(sort_by: str, url: str, api_key: str) -> json:
    """

    :return:
    """
    if not sort_by:
        sort_by = 'SALES'

    query_start = 'query TrendingCollections($first: Int) {'

    sorting_piece = f'contracts(orderBy: {sort_by}, orderDirection: DESC, first: $first)'

    query_body = """{
      edges {
        node {
          address
          ... on ERC721Contract {
            name
            stats {
              totalSales
              average
              ceiling
              floor
              volume
            }
            symbol
          }
        }
      }
      pageInfo {
        hasNextPage
        hasPreviousPage
        startCursor
        endCursor
      }
    }
  }"""

    full_query = query_start+sorting_piece+query_body
    headers = {'x-api-key': api_key}

    _json = {'query': full_query, 'variables': {'first': 50}}

    r = requests.post(url=url, json=_json, headers=headers)

    dat = r.json()

    response = [v for i in dat['data']['contracts']['edges'] for k, v in i.items() if i['node']['name'] != '']

    return [helper_functions.flatten(_dict) for _dict in response]


# def request_collection_stats():
#
#
