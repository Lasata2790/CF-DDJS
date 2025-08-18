import queue

# PriorityQueue: lower number = higher priority
job_queue = queue.PriorityQueue()
dlq_queue = queue.Queue()  # dead-letter queue

class Job:
    def __init__(self, job_type, payload, priority=5, max_retries=3):
        self.job_type = job_type
        self.payload = payload
        self.priority = priority
        self.attempts = 0
        self.max_retries = max_retries

def push_job(job: Job):
    job_queue.put((job.priority, job))


def pop_job(self):
        if not self.priority_queue.empty():
            return self.priority_queue.get()[1]  # Job object
        return None

def send_to_dlq(self, job: Job):
        self.dlq.put(job)
        print(f"Moved to Dead Letter Queue: {job.payload}")
