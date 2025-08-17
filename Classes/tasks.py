from celery import Celery
from .db import log_attempt, update_job_status
import time
import random
import json

# app = Celery(
#     'scheduler',
#     broker='redis://localhost:6379/0',
#     backend='redis://localhost:6379/1'
# )

app = Celery(
    'scheduler',
    broker='memory://',
    backend='rpc://'  # optional, can also use 'rpc://' or 'cache+memory://'
)

dead_letter_file = "dead_letter.log"

@app.task(bind=True, max_retries=None)
def send_email_task(self, job_id, payload, max_retries=3):
    attempt_no = self.request.retries + 1
    try:
        print(f"[Attempt {attempt_no}] Sending email to {payload['email']}...")
        time.sleep(1)  # simulate email sending
        if payload['email'].endswith("example.com"):
            raise Exception("Simulated email failure")
        print(f" Email sent to {payload['email']}")
        log_attempt(job_id, attempt_no, "succeeded")
        update_job_status(job_id, "succeeded", attempt_no)
    except Exception as exc:
        log_attempt(job_id, attempt_no, "failed", str(exc))
        if attempt_no >= max_retries:
            update_job_status(job_id, "failed", attempt_no)
            send_to_dead_letter(job_id, payload)
        else:
            update_job_status(job_id, "queued", attempt_no)
            raise self.retry(exc=exc, countdown=5)

def send_to_dead_letter(job_id, payload):
    with open(dead_letter_file, "a") as f:
        f.write(f"Job ID {job_id}: {json.dumps(payload)}\n")
    print(f" Moved to dead-letter queue: {payload['email']}")
