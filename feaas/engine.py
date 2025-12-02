from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import feaas.objects as objs


class AbstractPlusEngine(ABC):
    
    @abstractmethod
    def file_run_script_on_item(
        self, 
        absolute_location_key: str, 
        plus_script_object_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""
    
    @abstractmethod
    def file_run_script_on_directory(
        self, 
        absolute_prefix: str, 
        plus_script_object_id: str,
        data: Dict[str, Any],
        recursive: bool = False,
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""
    
    @abstractmethod
    def collection_run_script_on_item(
        self, 
        collection_object_id: str, 
        plus_script_object_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""
    
    @abstractmethod
    def collection_run_script_on_all(
        self, 
        collection_object_id: str, 
        plus_script_object_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""
    
    @abstractmethod
    def stream_run_script_on_item(
        self, 
        stream_id: str,
        plus_script_object_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""
    
    @abstractmethod
    def stream_run_script_on_all(
        self, 
        stream_id: str,
        plus_script_object_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""
    
    @abstractmethod
    def dashboard_run_script(
        self, 
        dashboard_object_id: str, 
        plus_script_object_id: str,
        data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None
    ) -> objs.Receipt:
        """"""