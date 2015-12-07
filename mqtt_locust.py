import random
import time
import sys

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


class DisconnectError(Exception):
    pass


class Message(object):

    def __init__(self, topic, payload, start_time, timeout, name):
        self.topic = topic
        self.payload = payload
        self.start_time = start_time
        self.timeout = timeout
        self.name = name

    def timed_out(self, total_time):
        return self.timeout is not None and total_time > self.timeout


class MQTTClient(mqtt.Client):

    def __init__(self, *args, **kwargs):
        super(MQTTClient, self).__init__(*args, **kwargs)
        self.on_publish = self._on_publish
        self.on_disconnect = self._on_disconnect
        self.mmap = {}

    def publish(self, topic, payload=None, repeat=1, name='mqtt', **kwargs):
        timeout = kwargs.pop('timeout', 5)
        for i in range(repeat):
            start_time = time.time()
            try:
                err, mid = super(MQTTClient, self).publish(
                    topic,
                    payload=payload,
                    **kwargs
                )
                if err:
                    raise ValueError(err)
                self.mmap[mid] = Message(
                        topic, payload, start_time, timeout, name
                        )
            except Exception as e:
                total_time = time.time() - start_time
                fire_locust_failure(
                    request_type='mqtt',
                    name=name,
                    response_time=total_time,
                    exception=e,
                )

    def _on_publish(self, client, userdata, mid):
        end_time = time.time()
        message = self.mmap.pop(mid, None)
        if message is None:
            return
        total_time = end_time - message.start_time
        if message.timed_out(total_time):
            fire_locust_failure(
                request_type='mqtt',
                name=message.name,
                response_time=total_time,
                exception=TimeoutError("publish timed out"),
            )
        else:
            fire_locust_success(
                request_type='mqtt',
                name=message.name,
                response_time=total_time,
                response_length=len(message.payload),
            )
        self.check_for_locust_timeouts(end_time)

    def _on_disconnect(self, client, userdata, rc):
        fire_locust_failure(
            request_type='mqtt',
            name=userdata,
            response_time=0,
            exception=DisconnectError("disconnected"),
        )
        self.reconnect()

    def check_for_locust_timeouts(self, end_time):
        timed_out = [mid for mid, msg in dict(self.mmap).iteritems()
                     if msg.timed_out(end_time - msg.start_time)]
        for mid in timed_out:
            msg = self.mmap.pop(mid)
            total_time = end_time - msg.start_time
            fire_locust_failure(
                request_type='mqtt',
                name=msg.name,
                response_time=total_time,
                exception=TimeoutError(
                    "message not received in %s s" % msg.timeout
                    ),
            )


class MQTTLocust(Locust):

    def __init__(self, *args, **kwargs):
        super(Locust, self).__init__(*args, **kwargs)
        if self.host is None:
            raise LocustError("You must specify a host")
        self.client = MQTTClient()
        try:
            [host, port] = self.host.split(":")
        except:
            host, port = self.host, 1883
        self.client.connect(host, port=port)
        self.client.loop_start()
