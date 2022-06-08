from locust import HttpUser, between, task

class UserBehavior(HttpUser):
    wait_time = between(1, 3)
    
    def on_start(self):
        pass
    
    @task(1)
    def index(self):
        self.client.get("/")
        