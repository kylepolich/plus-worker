from feaas.util import common
# from chalicelib.actions.load.docx2txt import Docx2Txt


class ElasticSearchUtil(object):


    def __init__(self, dao):
        self.blobstore = dao.get_blobstore()
        # self.docx2texthandler = Docx2Txt(dao)


    def is_indexable(self, key):
        ext = common.get_extension(key)
        if ext in ['.text', '.txt', '.md', '.json', '.docx', '.shtml', '.html']:
            return True
        else:
            return False


    def get_text(self, key):
        ext = common.get_extension(key)
        if ext in ['.text', '.txt', '.md', '.json']:
            content = self.blobstore.get_blob(key)
            return content.decode('utf-8')
        # elif ext == '.docx':
        #     content = self.blobstore.get_blob(key)
        #     receipt = self.docx2texthandler._libreoffice_convert(content)
        #     return receipt.outputs['text'].sval
        else:
            return None

