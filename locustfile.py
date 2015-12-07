import json
import random
import resource

from locust import TaskSet, task

from mqtt_locust import MQTTLocust

resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))


class MyTaskSet(TaskSet):
    @task(1)
    def test(self):
        self.client.publish(
                'lamp/set_config', self.payload(), qos=0, timeout=60, repeat=10
                )

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
