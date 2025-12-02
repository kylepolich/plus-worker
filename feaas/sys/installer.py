from google.protobuf.json_format import Parse
import json
import feaas.objects as objs
import time
import uuid


def _build_frame_panel(username, title, app_object_id, constructor_id, icon='fa-code'):
    elem = objs.DashboardElement(
        dtype=objs.DashboardElement.DashboardElementType.FRAME_PANEL,
        title=title,
        dapp=objs.DashboardAppShortcut(app_object_id=app_object_id),
        w=1,
        h=1,
        x=1,
        y=1,
        icon=icon,
        i=str(uuid.uuid4()),
        constructor_id=constructor_id)
    return elem


def build_dashboard_app(username, object_id, title, unique_id, icon, sort_order, app_name, app_object_id=None, tutorial=None):
    owner = f'{username}/dashboard'
    if app_object_id is None:
        app_object_id=f'{username}/{app_name}.default'
    app = _build_frame_panel(username, title, app_object_id, icon, f'sys.app.{app_name}')
    elements = [app]
    return objs.Dashboard(
        object_id=object_id,
        owner=owner,
        title=title,
        description='',
        unique_id=unique_id,
        icon=icon,
        layout=1,
        margin=0,
        deletable=False,
        read_only=True,
        tutorial=tutorial,
        sort_order=sort_order,
        elements=elements)


def build_forager_dashboard(username, title='My Files', unique_id='my_files', sort_order=0):
    object_id = f'{username}/dashboard.{unique_id}'
    return build_dashboard_app(username, object_id, title, unique_id, 'fa-folder-tree', sort_order, 'forager')


def build_explorer_dashboard(username, title='Explorer', unique_id='explorer', sort_order=0):
    object_id = f'{username}/dashboard.{unique_id}'
    return build_dashboard_app(username, object_id, title, unique_id, 'fa-database', sort_order, 'explorer')


def build_stream_dashboard(username, title='My Stream', sort_order=0, unique_id='my_stream'):
    object_id = f'{username}/dashboard.{unique_id}'
    return build_dashboard_app(username, object_id, title, unique_id, 'fa-rss', sort_order, 'stream')


def do_install(docstore, oc_object_id: str, username: str, dest_object_id: str, owner: str, input_data: dict) -> str:
    doc = docstore.get_document(oc_object_id)
    if doc is None:
        raise Exception(f'Record not found: {oc_object_id}')
    o = json.dumps(doc)
    oc = Parse(o, objs.ObjectConstructor())
    data = {}
    for page in oc.pages:
        for param in page.params:
            var_name = param.var_name
            if var_name not in input_data:
                if not(param.optional):
                    raise Exception(f"Cannot build without {var_name}")
                value=param.sdefault
            else:
                value=input_data[var_name]
            data[var_name] = value
    data['object_id'] = dest_object_id
    data['owner'] = owner
    docstore.save_document(dest_object_id, data)
    return dest_object_id

