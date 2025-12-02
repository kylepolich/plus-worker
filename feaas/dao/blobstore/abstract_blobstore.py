from abc import  abstractmethod
from feaas.dao import abstract_wiring as wiring


class AbstractBlobstore(wiring.Wiring):


    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name


    def get_bucketname(self) -> str:
        return self.bucket_name


    @abstractmethod
    def save_blob(self, key: str, contents: str):
        pass


    @abstractmethod
    def move(self, src_key: str, dest_key: str):
        pass


    @abstractmethod
    def get_url_from_key(self, key):
        """
        Return a url beginning with `https://` which can be used to retrieve the
        object via a web request.
        """
        pass

    @abstractmethod
    def get_key_from_url(self, url):
        pass


    @abstractmethod
    def delete_blob(self, key: str):
        pass


    @abstractmethod
    def get_blob(self, key: str):
        pass


    @abstractmethod
    def get_blob_metadata(self, key: str):
        pass


    @abstractmethod
    def get_keys_matching_pattern(self, pattern='*.crawl'):
        pass


    @abstractmethod
    def exists(self, key: str):
        pass
