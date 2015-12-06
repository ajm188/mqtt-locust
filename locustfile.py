import json
import random

from locust import TaskSet, task

from mqtt_locust import MQTTLocust


class MyTaskSet(TaskSet):
    @task(1)
    def test(self):
        self.client.publish('lamp/set_config', self.payload())

    def payload(self):
        payload = {
            'on': random.choice(['true', 'false']),
            'color': {
                'h': random.random(),
                's': random.random(),
            },
            'brightness': random.random(),
        }
        return json.dumps(payload)


class MyLocust(MQTTLocust):
    task_set = MyTaskSet
    min_wait = 5000
    max_wait = 15000
