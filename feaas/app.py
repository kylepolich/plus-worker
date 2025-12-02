import os
from chalice import Chalice, Response, UnauthorizedError
from chalicelib.plus_engine import AbstractPlusEngine

# Import blueprints
from chalicelib.files import files_bp
from chalicelib.collections import collections_bp
from chalicelib.streams import streams_bp
from chalicelib.dashboards import dashboards_bp
from chalicelib.scripts import scripts_bp

app = Chalice(app_name='vibe-api')

# PlusEngine instance - will be injected/configured based on environment
_plus_engine = None


def get_plus_engine() -> AbstractPlusEngine:
    """Get the PlusEngine instance"""
    global _plus_engine
    if _plus_engine is None:
        # TODO: Initialize your concrete PlusEngine implementation here
        # Example: _plus_engine = ConcretePlusEngine(config)
        raise RuntimeError("PlusEngine not initialized. Set it via set_plus_engine()")
    return _plus_engine


def set_plus_engine(engine: AbstractPlusEngine):
    """Set the PlusEngine instance (for testing or initialization)"""
    global _plus_engine
    _plus_engine = engine

# Register blueprints
app.register_blueprint(files_bp, url_prefix='/files')
app.register_blueprint(collections_bp, url_prefix='/collections')
app.register_blueprint(streams_bp, url_prefix='/streams')
app.register_blueprint(dashboards_bp, url_prefix='/dashboards')
app.register_blueprint(scripts_bp, url_prefix='/scripts')


def check_auth():
    """Simple token authentication check"""
    request = app.current_request
    auth_token = os.environ.get('API_AUTH_TOKEN', 'your-token-here')
    
    # Check Authorization header
    auth_header = request.headers.get('Authorization', '')
    
    if not auth_header.startswith('Bearer '):
        raise UnauthorizedError('Missing or invalid Authorization header')
    
    token = auth_header.replace('Bearer ', '')
    
    if token != auth_token:
        raise UnauthorizedError('Invalid token')
    
    return True


@app.route('/')
def index():
    """Root health check endpoint - no auth required"""
    return {
        'status': 'healthy',
        'service': 'vibe-api',
        'message': 'API is running smoothly ✨'
    }


@app.route('/health')
def health():
    """Additional health endpoint - no auth required"""
    return {
        'status': 'ok',
        'version': '1.0.0'
    }