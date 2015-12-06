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


class TimeoutError(ValueError):
    pass


class Message(object):

    def __init__(self, topic, payload, start_time, timeout):
        self.topic = topic
        self.payload = payload
        self.start_time = start_time
        self.timeout = timeout

    def timed_out(self, total_time):
        return self.timeout is not None and total_time > self.timeout


class MQTTClient(mqtt.Client):

    def publish(self, topic, payload=None, **kwargs):
        timeout = kwargs.pop('timeout', 5)
        if not hasattr(self, 'mmap'):
            self.mmap = {}
        start_time = time.time()
        try:
            err, mid = super(MQTTClient, self).publish(
                topic,
                payload=payload,
                **kwargs
            )
            if err:
                raise ValueError(err)
            self.mmap[mid] = Message(topic, payload, start_time, timeout)
        except Exception as e:
            total_time = time_delta(start_time, time.time())
            fire_locust_failure(
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
        total_time = time_delta(message.start_time, end_time)
        if message.timed_out(total_time):
            fire_locust_failure(
                request_type='mqtt',
                name='publish',
                response_time=total_time,
                exception=TimeoutException((timeout, total_time)),
            )
        else:
            fire_locust_success(
                request_type='mqtt',
                name='publish',
                response_time=total_time,
                response_length=len(message.payload),
            )
        self.check_for_locust_timeouts(end_time)

    def check_for_locust_timeouts(self, end_time):
        timed_out = [mid for mid, msg in self.mmap
                     if msg.timed_out(time_delta(msg.start_time, end_time))]
        for mid in timed_out:
            msg = self.mmap.pop(mid)
            total_time = time_delta(msg.start_time, end_time)
            fire_locust_failure(
                request_type='mqtt',
                name='publish',
                response_time=total_time,
                exception=TimeoutError((msg.timeout, total_time)),
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
