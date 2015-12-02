import random

import paho.mqtt.client as mqtt
from locust import Locust
from locust import task
from locust import TaskSet


class MQTTClient(mqtt.Client):

    def __getattr__(self, name):
        attr = mqtt.Client.__getattr__(self, name)
        if not callable(attr):
            return attr

        def wrapper(*args, **kwargs):
            # TODO: add timers for reporting back to locust
            # TODO: check return results
            return attr(*args, **kwargs)

        return wrapper


class MQTTLocust(Locust):

    def __init__(self, *args, **kwargs):
        super(Locust, self).__init__(*args, **kwargs)
        self.client = MQTTClient()


class ExampleMQTTClientBehavior(TaskSet):

    @task(1)
    def pub_to_set_config(self):
        self.client.single(
            self.topic,
            payload=self.payload(),
            qos=self.qos,
            retain=self.retain,
            hostname=self.hostname,
            port=self.port,
        )


class ExampleMQTTLoadTester(MQTTLocust):

    topic = 'lamp/set_config'
    qos = 1
    retain = False
    hostname = 'my.mqtt.host.sucks'
    port = 1883

    min_wait = 5
    max_wait = 500

    def payload(self):
        payload = {
            'on': random.choice(['true', 'false']),
            'color': {
                'h': random.random(),
                's': random.random(),
            },
            'brightness': random.random(),
        }
        return payload

    task_set = ExampleMQTTClientBehavior
