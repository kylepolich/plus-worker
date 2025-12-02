from feaas.sys.drip.step import DripCampaignStep
import json


class DripCampaign(object):


    def __init__(self, object_id, owner, title, from_address=None):
        self.object_id = object_id
        self.owner = owner
        self.title = title
        self.from_address = from_address
        self.steps = []


    def add_step(self, step: DripCampaignStep):
        self.steps.append(step)


    def toJSON(self):
        return self.__dict__


    def steps_from_json(self, jsteps,):
        for jstep in jsteps:
            step_id = jstep['step_id']
            send_delay_minutes = jstep['send_delay_minutes']
            subject = jstep['subject']
            body_key = jstep['body_key']
            step = DripCampaignStep(step_id, subject, body_key, self.from_address, send_delay_minutes)
            self.add_step(step)
