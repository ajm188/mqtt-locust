from locust import TaskSet, task

from mqtt_locust import MQTTLocust

class MyTaskSet(TaskSet):
    @task(1)
    def test(self):
        self.client.publish('/topic', 'Message')

class MyLocust(MQTTLocust):
    task_set = MyTaskSet
    min_wait = 5000
    max_wait = 15000
