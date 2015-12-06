import random
import time

import paho.mqtt.client as mqtt
from locust import Locust
from locust import task
from locust import TaskSet
from locust import events


class LocustError(Exception):
    pass


class MQTTClient(mqtt.Client):

    def sendMessage(self, topic, message):
        print(topic, message)

    def __getattribute__(self, name):
        attr = mqtt.Client.__getattribute__(self, name)
        if not callable(attr):
            return attr

        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = attr(*args, **kwargs)
            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(
                        request_type='mqtt', name=name,
                        response_time=total_time, exception=e
                        )
            else:
                total_time = int((time.time() - start_time) * 1000)
                # TODO: response_length
                events.request_success.fire(
                        request_type='mqtt', name=name,
                        response_time=total_time, response_length=0
                        )

        if name == 'publish':
            return wrapper
        else:
            return attr


class MQTTLocust(Locust):

    def __init__(self, *args, **kwargs):
        super(Locust, self).__init__(*args, **kwargs)
        if self.host is None:
            raise LocustError("You must specify a host")
        self.client = MQTTClient()
        self.client.connect(self.host)


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
