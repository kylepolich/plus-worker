from typing import List


def get_global_system_owners() -> List[str]:
    """
    Owners for global system-wide data that applies across all environments.
    
    Returns:
        List of global system owner strings
    """
    return [
        'sys.services-providers',
        'sys.env',
        'sys.oauth2.client',
    ]


def get_system_hostname_owners(hostname: str) -> List[str]:
    """
    Owners scoped to a particular hostname/environment.
    
    Args:
        hostname: The hostname/environment identifier
        
    Returns:
        List of hostname-specific owner strings
    """
    base_owners = [
        f'sys.user.{hostname}',
        f'sys.{hostname}.app',
        f'sys.{hostname}.users.groups',
        # TODO: Add f'sys.{hostname}.toolbar-params' when ready
    ]
    
    # Special case for neonpixel environment
    if hostname == 'app_neonpixel_co':
        base_owners.extend([
            'sys.app_neonpixel_co.clients',
            'sys@neonpixel.co/campaign-groups',
            'sys@neonpixel.co/creative-groups',
        ])
    
    return base_owners


def get_user_collection_owners(hostname: str, username: str) -> List[str]:
    """
    Owners for user-specific collections.
    
    Args:
        hostname: The hostname/environment identifier
        username: The username
        
    Returns:
        List of user collection owner strings
    """
    return [
        f'{hostname}/{username}/collection'
    ]

def get_user_app_owners(hostname: str, username: str) -> List[str]:
    """Returns user app-related owners."""
    return [f'sys.user.{hostname}.{username}.apps']

def get_user_stream_owners(hostname: str, username: str) -> List[str]:
    """
    Owners for user-specific streams.
    
    Args:
        hostname: The hostname/environment identifier
        username: The username
        
    Returns:
        List of user stream owner strings
    """
    return [
        f'{hostname}/{username}/stream'
    ]


def get_user_dashboard_owners(hostname: str, username: str) -> List[str]:
    """
    Owners for user-specific dashboards.
    
    Args:
        hostname: The hostname/environment identifier
        username: The username
        
    Returns:
        List of user dashboard owner strings
    """
    return [
        f'{hostname}/{username}/dashboard'
    ]


def get_user_api_gateway_owners(hostname: str, username: str) -> List[str]:
    """
    Owners for user-specific API gateway configurations.
    
    Args:
        hostname: The hostname/environment identifier
        username: The username
        
    Returns:
        List of user API gateway owner strings
    """
    return [
        f'{hostname}/{username}/api_gateway'
    ]


def get_user_file_owners(hostname: str, username: str) -> List[str]:
    """
    Owners for user-specific files and triggers.
    
    Args:
        hostname: The hostname/environment identifier
        username: The username
        
    Returns:
        List of user file owner strings
    """
    return [
        f'{hostname}/{username}/file_trigger'
    ]


def get_user_collection_extra_patterns(hostname: str, username: str, collection_id: str) -> List[str]:
    return [
        f'{hostname}/{username}/collection-input.{collection_id}',
        f'{hostname}/{username}/collections-shared.{collection_id}',
    ]
