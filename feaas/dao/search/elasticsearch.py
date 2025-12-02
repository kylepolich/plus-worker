# from feaas.dao.search.abstract_search import Search
# from elasticsearch import Elasticsearch, NotFoundError
# from datetime import datetime
# import traceback
# import os


# """
# {
#   query: {
#     bool: {
#       must: [
#         { match: { text: search } },
#         { wildcard: { 'key.keyword': `${path}*` } }
#       ]
#     }
#   }
# }
# """

# def analyze_uri(uri):
#     he = uri.find('://')
#     if he == -1:
#         schema = None
#         he = -3
#     else:
#         schema = uri[:he]
#     pe = uri.rfind(':')
#     if pe == -1:
#         port = None
#         pe = len(uri)
#     else:
#         port = int(uri[pe+1:])
#     host = uri[he+3:pe]
#     """
#     Get Host, Port, Schema from Uri.
#     ex: uri = https://test.domain.com:2345
#     will return dict
#     {
#         host: 'test.domain.com',
#         port: '2345',
#         schema: 'https'
#     }
#     """
#     return {
#         'host': host,
#         'port': port,
#         'schema': schema
#     }


# class ElasticSearchDao(Search):


#     def __init__(self, es_url='', es_user='', es_password='', index=None):
#         super().__init__(es_url, es_user, es_password)
#         self.index = index
#         url_info = analyze_uri(es_url)
#         if url_info['host'] is None or url_info['schema'] is None or url_info['port'] is None:
#             raise Exception(
#                 f"Can't find Host, Schema, Port in this URL: {es_url}")
#         if len(es_user) > 0:
#             self.es = Elasticsearch(
#                 [{
#                     'host': url_info['host'],
#                     'port': url_info['port'],
#                     "scheme": url_info['schema']
#                 }],
#                 http_auth=(es_user, es_password), verify_certs=False
#             )
#         else:
#             self.es = Elasticsearch([url_info['host']], scheme=url_info['schema'], port=url_info['port'])


#     def delete_index(self, index):
#         """
#         -> Reset functions. It will be enabled on only development mode
#         """
#         self.es.indices.delete(index)
#         return True


#     def create_index(self, index):
#         if not self.es.indices.exists(index):
#             self.es.indices.create(index)
#         return True


#     def update_timestamp(self, pre_data, data):
#         n = int(datetime.now().timestamp())
#         if pre_data is None:
#             pre_data = {}
#         if pre_data is None or 'created_at' not in pre_data['_source']:
#             data["created_at"] = data['updated_at'] = n
#         else:
#             data["updated_at"] = n
#             data = {**pre_data['_source'], **data}
#         return data


#     def save_document(self, id, contents, index=None, doc={}):
#         if doc is None:
#             doc = {}
#         if index is None:
#             index = self.index
#         n = int(datetime.now().timestamp())
#         doc["text"] = contents
#         doc["key"] = id
#         doc["updated_at"] = n
#         res = self.es.index(id=id, body=doc, index=index)
#         return res


#     def save_object(self, id, obj, index=None):
#         if index is None:
#             index = self.index
#         obj['key'] = id
#         obj['updated_at'] = int(datetime.now().timestamp())
#         res = self.es.index(id=id, body=obj, index=index)
#         return res


#     def save_dict(self, id, d, index=None):
#         if index is None:
#             index = self.index
#         n = int(datetime.now().timestamp())
#         d['updated_at'] = n
#         if 'key' not in d:
#             d['key'] = id
#         res = self.es.index(id=id, body=d, index=index)
#         return res


#     def delete_document(self, id, index=None):
#         if index is None:
#             index = self.index
#         res = self.es.delete(index, id)
#         return res


#     def get_document(self, id, index=None):
#         if index is None:
#             index = self.index
#         try:
#             res = self.es.get(index, id)
#             return res['_source']
#         except NotFoundError as e:
#             return None    
    
#     def exists(self, id, index=None):
#         if index is None:
#             index = self.index
#         try:
#             res = self.es.exists(index, id)
#             return res
#         except NotFoundError as e:
#             return False


#     def search(self, q, limit=100, offset=0, index=None):
#         if index is None:
#             index = self.index
#         sz_lm_query = {
#             "from": offset,
#             "size": limit
#         }
#         if q:
#             if type(q) is str:
#                 body = {
#                     'query': {
#                         'query_string': {
#                             "query": q
#                         }
#                     },
#                     **sz_lm_query
#                 }
#             elif type(q) is dict:
#                 body = {'query': q, **sz_lm_query}
#             else:
#                 body = {'query': {'match_all': {}}, **sz_lm_query}
#         else:
#             body = {'query': {'match_all': {}}, **sz_lm_query}
#         return self.do_search(body, index)


#     def do_search(self, body, index=None):
#         if index is None:
#             index = self.index
#         try:
#             result = self.es.search(index=index, body=body)
#         except NotFoundError:
#             msg = traceback.format_exc()
#             print(f'**** Cannot find elastic search index {index}.  Creating it ****')
#             return {"results": [], "took": -1, "error": msg}
#         took = result['took']
#         inner = result['hits']
#         hits = inner['hits']
#         results = []
#         for hit in hits:
#             results.append({
#                 "id": hit['_id'],
#                 "score": hit['_score'],
#                 "doc": hit['_source']
#             })
#         return {"results": results, "took": took}

#     def update_document(self, id, index=None, doc={}):
#         if doc is None:
#             doc = {}
#         if index is None:
#             index = self.index

#         n = int(datetime.now().timestamp())
#         doc["updated_at"] = n

#         res = self.es.update(index, id, body={"doc": doc})

#         return res
