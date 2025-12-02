from abc import ABC, abstractmethod


class PlusEngine(ABC):
    """
    Abstract base class for PlusEngine implementations.
    
    PlusEngine provides a unified interface for managing files, collections, streams,
    actions, jobs, and dashboards across different backend implementations.
    Each implementation must provide hostname and username for multi-tenant support.
    
    Args:
        hostname (str): The hostname identifier for the tenant/environment
        username (str): The username identifier for the authenticated user
    """
    
    def __init__(self, hostname, username):
        self.hostname = hostname
        self.username = username
        # TODO: has a loaded list of available Actions based on update_actions
        # TODO: each app exposes an endpoint to get the available actions
        # TODO: a way to consolidate all of them by calling the API endpoints

    def _validate_script_object_id(self, script_object_id):
        """
        Validate that the script_object_id is accessible by current user.
        
        Args:
            script_object_id (str): Full object_id of the script
            
        Returns:
            bool: True if valid, False otherwise
            
        Raises:
            ValueError: If script_object_id is not accessible by current user
        """
        if not script_object_id:
            raise ValueError("script_object_id is required")
        
        user_prefix = f"{self.hostname}/{self.username}/"
        public_prefix = f"{self.hostname}/public/"
        
        if not (script_object_id.startswith(user_prefix) or script_object_id.startswith(public_prefix)):
            raise ValueError(
                f"Script access denied. script_object_id must start with '{user_prefix}' or '{public_prefix}', "
                f"got: {script_object_id}"
            )
        
        return True

    @abstractmethod
    def run_script(self, script_object_id, data):
        """
        Execute a script directly by its object_id.
        
        This is a general-purpose script execution method that runs a script
        without being tied to any specific collection or stream context.
        
        Args:
            script_object_id (str): Full object_id of the script to execute (must start with {hostname}/{username}/ or {hostname}/public/)
            data (dict): Input parameters to pass to the script
            
        Returns:
            dict: Script execution results containing:
                - 'job_id' (str): Identifier for the background job (if async)
                - 'status' (str): Execution status (success, error, pending)
                - 'result' (any): Script execution result
                - 'executed_at' (datetime): Script execution timestamp
                - 'duration' (float): Execution duration in seconds
        """
        pass

    ### Files -----------------------------------------------------------------
    
    @abstractmethod
    def file_exists(self, absolute_location_key):
        """
        Check if a file exists at the specified location.
        
        Args:
            absolute_location_key (str): The absolute path/key identifying the file location
            
        Returns:
            bool: True if the file exists, False otherwise
        """
        pass

    @abstractmethod
    def file_get(self, absolute_location_key):
        """
        Retrieve the contents of a file from the specified location.
        
        Args:
            absolute_location_key (str): The absolute path/key identifying the file location
            
        Returns:
            bytes or str: The file contents, or None if file doesn't exist
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            PermissionError: If access is denied
        """
        pass

    @abstractmethod
    def file_delete(self, absolute_location_key):
        """
        Delete a file from the specified location.
        
        Args:
            absolute_location_key (str): The absolute path/key identifying the file location
            
        Returns:
            bool: True if successfully deleted, False if file didn't exist
            
        Raises:
            PermissionError: If deletion is not allowed
        """
        pass

    @abstractmethod
    def file_folder_list(self, location_prefix, limit=None, recursive=False, continuation_token=None):
        """
        List all files in a folder/directory.
        
        Args:
            location_prefix (str): The folder path prefix to list files from
            limit (int, optional): Maximum number of files to return. Defaults to None (no limit)
            recursive (bool, optional): Whether to list files recursively in subdirectories. Defaults to False
            continuation_token (str, optional): Token for paginated results continuation. Defaults to None
            
        Returns:
            dict: Dictionary containing:
                - 'files' (list): List of file information dictionaries
                - 'continuation_token' (str): Token for next page, if more results available
                - 'truncated' (bool): Whether results were truncated due to limit
        """
        pass

    # TODO: file_folder_run_script
    # TODO: file_run_script

    ### Collections -----------------------------------------------------------
    
    @abstractmethod
    def collections_list(self):
        """
        Return a list of all Collections for the current hostname/username.
        
        Collections are organized containers of related items that can be
        queried, modified, and have scripts executed against them.
        
        Returns:
            list: List of collection dictionaries, each containing:
                - 'collection_key' (str): Unique identifier for the collection
                - 'name' (str): Human-readable collection name
                - 'description' (str): Collection description
                - 'item_count' (int): Number of items in the collection
                - 'created_at' (datetime): Collection creation timestamp
                - 'modified_at' (datetime): Last modification timestamp
        """
        pass

    @abstractmethod
    def collections_items_list(self, collection_key, limit=None, continuation_token=None):
        """
        Return a list of all items in the specified collection.
        
        Args:
            collection_key (str): Unique identifier for the collection
            limit (int, optional): Maximum number of items to return. Defaults to None
            continuation_token (str, optional): Token for paginated results continuation. Defaults to None
            
        Returns:
            dict: Dictionary containing:
                - 'items' (list): List of item dictionaries from the collection
                - 'continuation_token' (str): Token for next page, if available
                - 'truncated' (bool): Whether results were truncated
        """
        pass

    @abstractmethod
    def collections_items_run_script(self, collection_key, script_object_id, data):
        """
        Execute a script against all items in the specified collection.
        
        Args:
            collection_key (str): Unique identifier for the collection
            script_object_id (str): Full object_id of the script to execute (must start with {hostname}/{username}/ or {hostname}/public/)
            data (dict): Additional input parameters to pass to the script
            
        Returns:
            dict: Script execution results containing:
                - 'job_id' (str): Identifier for the background job
                - 'status' (str): Execution status
                - 'results' (list): Results from script execution on each item
        """
        pass

    @abstractmethod
    def collections_item_add(self):
        """
        Add a new item to a collection.
        
        Returns:
            dict: Dictionary containing the created item information:
                - 'item_id' (str): Unique identifier for the new item
                - 'collection_key' (str): Parent collection identifier
                - 'created_at' (datetime): Item creation timestamp
        """
        pass

    @abstractmethod
    def collections_item_delete(self):
        """
        Delete an item from a collection.
        
        Returns:
            bool: True if item was successfully deleted, False otherwise
        """
        pass

    @abstractmethod
    def collections_item_update(self):
        """
        Update an existing item in a collection.
        
        Returns:
            dict: Dictionary containing the updated item information:
                - 'item_id' (str): Unique identifier for the item
                - 'collection_key' (str): Parent collection identifier
                - 'modified_at' (datetime): Last modification timestamp
        """
        pass


    @abstractmethod
    def collections_item_add(self, collection_key, item_data):
        """
        Add a new item (record) to a collection.
        
        Args:
            collection_key (str): Unique identifier for the parent collection
            item_data (dict): Key-value pairs representing the item's fields
        
        Returns:
            dict: Dictionary containing the created item information:
                - 'item_id' (str): Unique identifier for the new item
                - 'collection_key' (str): Parent collection identifier
                - 'created_at' (datetime): Item creation timestamp
                - 'data' (dict): The stored item data
        """
        pass


    @abstractmethod
    def collections_item_run_script(self, unique_id, script_object_id, data):
        """
        Execute a script against a specific item in a collection.
        
        Args:
            unique_id (str): Short unique identifier for the target item
            script_object_id (str): Full object_id of the script to execute (must start with {hostname}/{username}/ or {hostname}/public/)
            data (dict): Additional input parameters to pass to the script
        
        Returns:
            dict: Script execution results containing:
                - 'item_id' (str): Target item identifier
                - 'status' (str): Execution status
                - 'result' (any): Result from script execution
                - 'executed_at' (datetime): Script execution timestamp
        """
        pass

    ### Credentials -----------------------------------------------------------

    @abstractmethod
    def credentials_list(self, limit=None, continuation_token=None):
        """
        Return a list of all credentials available.
        """
        pass


    ### Streams ---------------------------------------------------------------
    
    @abstractmethod
    def streams_list(self):
        """
        Return a list of all Streams for the current hostname/username.
        
        Streams are real-time data flows that can be monitored, processed,
        and have scripts executed against incoming data.
        
        Returns:
            list: List of stream dictionaries, each containing:
                - 'stream_key' (str): Unique identifier for the stream
                - 'name' (str): Human-readable stream name
                - 'description' (str): Stream description
                - 'status' (str): Stream status (active, paused, stopped)
                - 'item_count' (int): Total items processed
                - 'created_at' (datetime): Stream creation timestamp
        """
        pass

    @abstractmethod
    def streams_items_list(self, stream_key, limit=None, continuation_token=None):
        """
        Return a list of recent items from the specified stream.
        
        Args:
            stream_key (str): Unique identifier for the stream
            limit (int, optional): Maximum number of items to return. Defaults to None
            continuation_token (str, optional): Token for paginated results continuation. Defaults to None
            
        Returns:
            dict: Dictionary containing:
                - 'items' (list): List of recent stream items
                - 'continuation_token' (str): Token for next page, if available
                - 'truncated' (bool): Whether results were truncated
        """
        pass

    @abstractmethod
    def streams_items_run_script(self, stream_key, script_object_id, data):
        """
        Execute a script against items in the specified stream.
        
        Args:
            stream_key (str): Unique identifier for the stream
            script_object_id (str): Full object_id of the script to execute (must start with {hostname}/{username}/ or {hostname}/public/)
            data (dict): Additional input parameters to pass to the script
            
        Returns:
            dict: Script execution results containing:
                - 'job_id' (str): Identifier for the background job
                - 'status' (str): Execution status
                - 'processed_count' (int): Number of items processed
        """
        pass

    @abstractmethod
    def streams_item_add(self, stream_key, timestamp, item):
        """
        Add a new item to a stream.
        
        Args:
            stream_key (str): Unique identifier for the stream
            timestamp (int): Timestamp for the stream item
            item (dict): Item data to add to the stream
        
        Returns:
            dict: Dictionary containing the added item information:
                - 'stream_id' (str): Full stream identifier
                - 'stream_key' (str): Parent stream identifier
                - 'timestamp' (int): Item timestamp
        """
        pass

    @abstractmethod
    def streams_item_delete(self, stream_key, timestamp):
        """
        Delete an item from a stream.
        
        Args:
            stream_key (str): Unique identifier for the stream
            timestamp (int): Timestamp of the item to delete
        
        Returns:
            bool: True if item was successfully deleted, False otherwise
        """
        pass

    @abstractmethod
    def streams_item_update(self, stream_key, timestamp, update):
        """
        Update an existing item in a stream.
        
        Args:
            stream_key (str): Unique identifier for the stream
            timestamp (int): Timestamp of the item to update
            update (dict): Dictionary of fields to update
        
        Returns:
            dict: Dictionary containing the updated item information:
                - 'stream_id' (str): Full stream identifier
                - 'stream_key' (str): Parent stream identifier
                - 'timestamp' (int): Item timestamp
                - 'modified_at' (datetime): Last modification timestamp
        """
        pass

    @abstractmethod
    def streams_item_run_script(self):
        """
        Execute a script against a specific item in a stream.
        
        Returns:
            dict: Script execution results containing:
                - 'item_id' (str): Target item identifier
                - 'status' (str): Execution status
                - 'result' (any): Result from script execution
                - 'executed_at' (datetime): Script execution timestamp
        """
        pass

    ### Actions ---------------------------------------------------------------
    
    @abstractmethod
    def actions_search(self, q):
        """
        Search for available actions using a query string.
        
        Actions are executable operations that can be invoked with parameters.
        They may be provided by different applications or modules.
        
        Args:
            q (str): Search query string to match against action names, descriptions, or tags
            
        Returns:
            list: List of matching action dictionaries, each containing:
                - 'action_id' (str): Unique identifier for the action
                - 'name' (str): Human-readable action name
                - 'description' (str): Action description
                - 'parameters' (list): List of required/optional parameters
                - 'tags' (list): List of tags for categorization
        """
        pass

    @abstractmethod
    def actions_action_run(self, action_id, data):
        """
        Execute a specific action by its identifier.
        
        Args:
            action_id (str): Unique identifier for the action to execute
            
        Returns:
            dict: Action execution results containing:
                - 'job_id' (str): Identifier for the background job (if async)
                - 'status' (str): Execution status (success, error, pending)
                - 'result' (any): Action execution result
                - 'executed_at' (datetime): Action execution timestamp
                - 'duration' (float): Execution duration in seconds
        """
        pass

    ### Jobs ------------------------------------------------------------------
    # TODO: define functions for jobs
    
    @abstractmethod
    def jobs_list(self):
        """
        Return a list of all jobs for the current hostname/username.
        
        Jobs represent background or scheduled tasks that can be monitored
        and managed independently of the originating request.
        
        Returns:
            list: List of job dictionaries, each containing:
                - 'job_id' (str): Unique identifier for the job
                - 'name' (str): Human-readable job name
                - 'status' (str): Job status (pending, running, completed, failed)
                - 'progress' (float): Completion percentage (0.0 to 1.0)
                - 'created_at' (datetime): Job creation timestamp
                - 'started_at' (datetime): Job start timestamp
                - 'completed_at' (datetime): Job completion timestamp
        """
        pass
    
    @abstractmethod
    def jobs_get(self, job_id):
        """
        Get detailed information about a specific job.
        
        Args:
            job_id (str): Unique identifier for the job
            
        Returns:
            dict: Detailed job information including logs and results
        """
        pass
    
    @abstractmethod
    def jobs_cancel(self, job_id):
        """
        Cancel a running or pending job.
        
        Args:
            job_id (str): Unique identifier for the job to cancel
            
        Returns:
            bool: True if job was successfully cancelled, False otherwise
        """
        pass

    ### Dashboards ------------------------------------------------------------
    # TODO: define functions for dashboards / sessions
    
    @abstractmethod
    def dashboards_list(self):
        """
        Return a list of all dashboards for the current hostname/username.
        
        Dashboards provide interactive interfaces for data visualization,
        monitoring, and control of collections, streams, and actions.
        
        Returns:
            list: List of dashboard dictionaries, each containing:
                - 'dashboard_id' (str): Unique identifier for the dashboard
                - 'name' (str): Human-readable dashboard name
                - 'description' (str): Dashboard description
                - 'status' (str): Dashboard status (active, inactive)
                - 'created_at' (datetime): Dashboard creation timestamp
        """
        pass
    
    @abstractmethod
    def dashboards_create_session(self, dashboard_id):
        """
        Create a new interactive session for a dashboard.
        
        Args:
            dashboard_id (str): Unique identifier for the dashboard
            
        Returns:
            dict: Session information containing:
                - 'session_id' (str): Unique identifier for the session
                - 'dashboard_id' (str): Parent dashboard identifier
                - 'expires_at' (datetime): Session expiration timestamp
                - 'websocket_url' (str): WebSocket URL for real-time updates
        """
        pass
    
    @abstractmethod
    def dashboards_end_session(self, session_id):
        """
        End an active dashboard session.
        
        Args:
            session_id (str): Unique identifier for the session to end
            
        Returns:
            bool: True if session was successfully ended, False otherwise
        """
        pass