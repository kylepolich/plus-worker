"""Screenshot action - capture a URL as PNG via Playwright (chromium)."""
import os
import re
import traceback
import uuid
from datetime import datetime, timezone

from feaas.abstract import AbstractAction
import feaas.objects as objs


class Screenshot(AbstractAction):

    def __init__(self, dao):
        v_w_min = objs.Validation(vtype=objs.ValidationType.GREATER_THAN, ival=50)
        v_w_max = objs.Validation(vtype=objs.ValidationType.LESS_THAN, ival=1920)
        v_h_min = objs.Validation(vtype=objs.ValidationType.GREATER_THAN, ival=50)
        v_h_max = objs.Validation(vtype=objs.ValidationType.LESS_THAN, ival=1080)
        params = [
            objs.Parameter(var_name='url', label='URL',
                           hint='https://wikipedia.org',
                           ptype=objs.ParameterType.URL),
            objs.Parameter(var_name='width', label='Width',
                           idefault=1200, ptype=objs.ParameterType.INTEGER,
                           validations=[v_w_min, v_w_max]),
            objs.Parameter(var_name='height', label='Height',
                           idefault=800, ptype=objs.ParameterType.INTEGER,
                           validations=[v_h_min, v_h_max]),
            objs.Parameter(var_name='dest_prefix', label='Destination',
                           sdefault='{hostname}/{username}/screenshots/',
                           ptype=objs.ParameterType.PREFIX),
        ]
        png_only = objs.Validation(vtype=objs.ValidationType.ENDS_WITH, svals=['.png'])
        outputs = [
            objs.Parameter(var_name='dest_key', label='Destination',
                           ptype=objs.ParameterType.KEY, validations=[png_only]),
        ]
        super().__init__(params, outputs)
        self.blobstore = dao.get_blobstore() if dao else None

    def execute_action(self, url, width, height, dest_prefix) -> objs.Receipt:
        if re.match(r'^http[:|/]', url):
            url = 'https://' + url.rsplit('/', 1)[-1]
        elif not re.match(r'^https', url):
            url = 'https://' + url

        if not dest_prefix.endswith('/'):
            dest_prefix += '/'
        dt = datetime.now(timezone.utc).strftime('%Y-%m-%d-at-%H-%M-%S')
        uid = str(uuid.uuid4())[:4]
        dest_key = f'{dest_prefix}{dt}-{uid}.png'

        local_path = f'/tmp/screenshot_{uuid.uuid4()}.png'
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(args=['--no-sandbox', '--disable-gpu'])
                context = browser.new_context(viewport={'width': width, 'height': height})
                page = context.new_page()
                page.goto(url, wait_until='networkidle', timeout=30000)
                page.screenshot(path=local_path, full_page=False)
                browser.close()
        except Exception:
            return objs.Receipt(success=False, error_message=traceback.format_exc())

        try:
            with open(local_path, 'rb') as f:
                img = f.read()
            self.blobstore.save_blob(dest_key, img, metadata={'url': url}, content_type='image/png')
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

        outputs = {'dest_key': objs.AnyType(ptype=objs.ParameterType.KEY, sval=dest_key)}
        return objs.Receipt(success=True, outputs=outputs, primary_output='dest_key')
