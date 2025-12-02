from abc import ABC, abstractmethod


class FeaasCore(ABC):


    def __init__(self, hostname, username):
        self.hostname = hostname
        self.username = username
        # TODO: has a loaded list of available Actions based on update_actions
        # TODO: each app exposes an endpoint to get the available actions
        # TODO: a way to consolidate all of them by calling the API endpoints


    ### Files -----------------------------------------------------------------

    @abstractmethod
    def file_exists(self, absolute_location_key):
        """"""


    @abstractmethod
    def file_get(self, absolute_location_key):
        """"""


    @abstractmethod
    def file_delete(self, absolute_location_key):
        """Delete a file"""


    @abstractmethod
    def file_folder_list(self, location_prefix, limit=None, recursive=False, continuation_token=None):
        """List all the files in a folder"""


    ### Collections -----------------------------------------------------------

    @abstractmethod
    def collections_list(self):
        """Return a list of all the Collections for the hostname/username"""


    @abstractmethod
    def collections_items_list(self, collection_key, limit=None, continuation_token=None):
        """Return a list of all items in the collection_key"""

    @abstractmethod
    def collections_items_run_script(self, collection_key):
        """"""

    @abstractmethod
    def collections_item_add(self):
        """"""

    @abstractmethod
    def collections_item_delete(self):
        """"""

    @abstractmethod
    def collections_item_update(self):
        """"""

    @abstractmethod
    def collections_item_run_script(self):
        """"""

    ### Streams ---------------------------------------------------------------

    @abstractmethod
    def streams_list(self):
        """Return a list of all the Streams for the hostname/username"""


    @abstractmethod
    def streams_items_list(self, collection_key, limit=None, continuation_token=None):
        """Return a list of all items in the collection_key"""


    @abstractmethod
    def streams_items_run_script(self, collection_key):
        """"""

    @abstractmethod
    def streams_item_add(self):
        """"""

    @abstractmethod
    def streams_item_delete(self):
        """"""

    @abstractmethod
    def streams_item_update(self):
        """"""

    @abstractmethod
    def streams_item_run_script(self):
        """"""

    ### Actions ---------------------------------------------------------------

    @abstractmethod
    def actions_search(self, q):
        """"""

    @abstractmethod
    def actions_action_run(self, action_id, data):
        """"""

    ### Jobs ------------------------------------------------------------------

    # TODO: define functions for jobs

    ### Dashboards ------------------------------------------------------------

    # TODO: define functions for dashboards / sessions

