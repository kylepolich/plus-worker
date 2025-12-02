class DripCampaignStep(object):


    def __init__(self, step_id, subject, body_key, from_address, send_after_minutes):
        self.step_id = step_id
        self.subject = subject
        self.body_key = body_key
        self.from_address = from_address
        self.send_after_minutes = send_after_minutes
