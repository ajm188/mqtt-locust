import json
import random
import resource

from locust import TaskSet, task

from mqtt_locust import MQTTLocust

resource.setrlimit(resource.RLIMIT_NOFILE, (999999, 999999))

TIMEOUT = 6
REPEAT = 100

class MyTaskSet(TaskSet):
    @task(1)
    def qos2(self):
        self.client.publish(
                'lamp/set_config', self.payload(), qos=2, timeout=TIMEOUT,
                repeat=REPEAT, name='qos2 publish'
                )
    @task(1)
    def qos1(self):
        self.client.publish(
                'lamp/set_config', self.payload(), qos=1, timeout=TIMEOUT,
                repeat=REPEAT, name='qos1 publish'
                )
    @task(1)
    def qos0(self):
        self.client.publish(
                'lamp/set_config', self.payload(), qos=0, timeout=TIMEOUT,
                repeat=REPEAT, name='qos0 publish'
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
