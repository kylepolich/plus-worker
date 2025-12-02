from abc import abstractmethod
from feaas.dao.search.abstract_search import Search


class LocalSearch(Search):
    """
    A collection of services which are likely to be provided by a variety
    of service providers
    """


    def __init__(self):
        super().__init__()
        self._database = {}


    def search(self, q):
        if not self.index:
            return None
        all_documents = self._database.get(self.index)
        if q:
            some_documents = []
            for doc in all_documents:
                if q in doc['_source']['body']:
                    some_documents.append(doc)
            return some_documents
        else:
            return all_documents


    def delete_index(self, index):
        if index in self._database:
            del self._database[index]
            return True
        return False


    def create_index(self, index):
        if index not in self._database:
            self._database[index] = []
            return True
        return False


    def set_index(self, index):
        self.index = index
        self.create_index(self.index)


    def update_document(self, id, contents):
        res = self.get_document(id)
        if res is None:
            return False
        document = {}
        document['_index'] = self.index
        document['_type'] = self.doc_type
        document['_id'] = id
        document['_source'] = contents
        document['result'] = 'updated'
        self._database[self.index].append(document)
        return document


    def save_document(self, id, contents):
        pre_data = self.get_document(id)
        contents = self.update_timestamp(pre_data, contents)
        if pre_data is None:
            res = self.create_document(id, contents)
        else:
            res = self.update_document(id, contents)

        return res


    def delete_document(self, id):
        all_documents = self._database.get(self.index)
        for i in range(len(all_documents)):
            doc = all_documents[i]
            if doc['_id'] == id:
                res = all_documents.pop(i)
                res['result'] = 'deleted'
                return res
        return False


    def get_document(self, id):
        all_documents = self._database.get(self.index)
        for doc in all_documents:
            if doc['_id'] == id:
                doc['found'] = True
                return doc
        return None
