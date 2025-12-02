from boto3.dynamodb.conditions import Key
from feaas.sys.executor import ActionExecutor
from feaas.sys.drip.campaign import DripCampaign
import feaas.objects as objs
import json
import time


class DripCampaignRunner(object):


    def __init__(self, dao, send_as_file=False):
        self.dao = dao
        self.send_as_file = send_as_file
        self.blobstore = dao.get_blobstore()
        self.docstore = dao.get_docstore()
        self.executor = ActionExecutor(dao)


    def run(self, now=None):
        if now is None:
            now = int(time.time() * 1000)
        r = { }
        # TODO: run other drips too
        for drip_object_id in ['sys.drip.welcome']:
            r[drip_object_id] = self._process_drip_campaign(drip_object_id, now)
        return r


    def run_test(self, drip: str, step_id, to_email):
        # Used exclusively in chalicelib.actions.sys.drip.send_test
        slug = drip
        key = f'sys/content/drip/{slug}/{slug}.drip.json'
        s = self.blobstore.get_blob(key)
        if s is None:
            return objs.Receipt(success=False, error_message=f'Could not find {key}')
        o = json.loads(s)
        from_address = 'portal@dataskeptic.com'
        if 'title' in o:
            title = o['title']
        else:
            title = 'Untitled'
        campaign = DripCampaign(None, None, title, from_address)
        campaign.steps_from_json(o['steps'])
        for step in campaign.steps:
            if step.step_id == step_id:
                return self._send_email(to_email, step)
        return objs.Receipt(success=False, error_message=f'Could not find step {step_id} in {drip}.')


    def _process_drip_campaign(self, owner, now):
        # TODO: handle big long lists
        # while 'LastEvaluatedKey' in response:
        #     response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        resp = self.docstore.table.query(
            IndexName="ownerIndex",
            KeyConditionExpression=Key('owner').eq(owner)
        )
        print(resp['Count'], resp['ScannedCount'], resp['ResponseMetadata'])
        arr = owner.split('.')
        slug = arr[2]
        key = f'sys/content/drip/{slug}/{slug}.drip.json'
        s = self.blobstore.get_blob(key)
        o = json.loads(s)
        from_address = 'portal@dataskeptic.com'
        campaign = DripCampaign(None, None, o['title'], from_address)
        campaign.steps_from_json(o['steps'])
        doc = self.docstore.get_document(owner)
        if doc is None:
            doc = { "last_run_at": 0 }
        last_run_at = doc['last_run_at']
        cool_off_ms = 1000 * 60 * 60 * 24
        update_after = last_run_at + cool_off_ms
        # TODO: set to last time run / save it here
        r = { 'sent_count': 0 }
        print('_process_drip_campaign', len(resp['Items']))
        to_send = []
        skips = []
        # TODO: handle user global unsubscribes, not just drip enrollment.
        for item in resp['Items']:
            object_id = item['object_id']
            if 'last_updated_at' in item:
                last_updated_at = item['last_updated_at']
            else:
                last_updated_at = 0
            if last_updated_at < update_after:
                to_send.append(object_id)
            else:
                skips.append(last_updated_at)
        print('_process_drip_campaign', len(to_send), len(skips), update_after)
        for object_id in to_send:
            sent = self.check_and_send(campaign, object_id, now)
            if sent:
                r['sent_count'] += 1
        doc['last_send_count'] = r['sent_count']
        doc['last_run_at'] = update_after
        self.docstore.save_document(owner, doc)
        return r


    def check_and_send(self, campaign: DripCampaign, object_id, now) -> bool: # email_was_sent
        doc = self.docstore.get_document(object_id)
        if 'to_email' in doc:
            to_email = doc['to_email']
        elif 'email' in doc:
            to_email = doc['email']
        else:
            print('Invalid record:', object_id)
            return False
        if 'started_drip_at_ts' not in doc:
            doc['started_drip_at_ts'] = now
        for step in campaign.steps:
            should_send = self._should_send(doc, step, now)
            if should_send:
                print('verified and sending to', to_email)
                r = self._send_email(to_email, step)
                sent = r.success
                doc['last_updated_at'] = now
                doc[step.step_id] = { "sent_ts": now }
                self.docstore.save_document(object_id, doc)
                return True
        doc['last_updated_at'] = now
        self.docstore.save_document(object_id, doc)
        return False


    def _should_send(self, doc, step, now):
            step_id = step.step_id
            if step_id in doc:
                return False
            send_after_seconds = step.send_after_minutes * 60
            dur = now - doc['started_drip_at_ts']
            if dur >= send_after_seconds:
                print('send? yes')
                return True
            else:
                print('send? time', dur, send_after_seconds)
                return False


    def _send_email(self, to_address, step):
        from_address = step.from_address
        username = 'portal@dataskeptic.com'
        subject = step.subject
        body_key = step.body_key
        body = self.blobstore.get_blob(body_key)
        if body is None:
            err = f'No content found in {body_key}'
            print(err)
            return objs.Receipt(success=False, error_message=err)
        body = body.decode('utf-8')
        # TODO: migrate to kicking off with action_executor instead
        if self.send_as_file:
            ts = int(time.time() * 1000)
            dest_key = f'sys/email/outbox/{from_address}/{to_address}/{ts}_{step.step_id}.mail'
            metadata = { "subject": subject, "from_address": from_address, "to_address": to_address }
            self.blobstore.save_blob(dest_key, body, metadata=metadata)
            outputs = {
                "dest_key": objs.AnyType(ptype=objs.ParameterType.KEY, sval=dest_key)
            }
            r = objs.Receipt(success=True, outputs=outputs)
        else:
            template = 'standard'
            data = {
                'username': username,
                'from_address': from_address,
                'to_address': to_address,
                'subject': subject,
                'body': body,
                'template': template
            }
            action_id = 'feaas-py.chalicelib.actions.vendor.sendgrid.send_email.SendEmail'
            r = self.executor.begin_action_execution(action_id, username, data)
        return r

