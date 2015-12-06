import random
import time

import paho.mqtt.client as mqtt
from locust import Locust
from locust import task
from locust import TaskSet
from locust import events


def time_delta(t1, t2):
    return int((t2 - t1) * 1000)


def fire_locust_failure(**kwargs):
    events.request_failure.fire(**kwargs)


def fire_locust_success(**kwargs):
    events.request_success.fire(**kwargs)


class LocustError(Exception):
    pass


class Message(object):

    def __init__(self, topic, payload, start_time):
        self.topic = topic
        self.payload = payload
        self.start_time = start_time


class MQTTClient(mqtt.Client):

    def publish(self, topic, payload=None, **kwargs):
        start_time = time.time()
        try:
            err, mid = super(MQTTClient, self).publish(
                topic,
                payload=payload,
                **kwargs
            )
            if err:
                raise ValueError(err)
            self.mmap = {} if not hasattr(self, 'mmap') else self.mmap
            self.mmap[mid] = Message(topic, payload, start_time)
            # TODO: need to have another thread that loops and looks for
            # timed-out messages and marks them as failed
        except Exception as e:
            total_time = time_delta(start_time, time.time())
            events.request_failure.fire(
                request_type='mqtt',
                name='publish',
                response_time=total_time,
                exception=e,
            )

    def on_publish(self, client, userdata, mid):
        end_time = time.time()
        message = self.mmap.pop(mid, None)
        if message is None:
            return
        events.request_success.fire(
            request_type='mqtt',
            name='publish',
            response_time = time_delta(message.start_time, end_time),
            response_length = len(message.payload),
        )

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
        self.client.loop_start()
