from celery import Celery
from .db import log_attempt, update_job_status, log_to_dlq
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
def execute_job_task(self, job_id, job_type, payload, max_retries=3):
    attempt_no = self.request.retries + 1
    try:
        print(f"[Attempt {attempt_no}] Executing {job_type} for payload: {payload}")
        time.sleep(1)

        if job_type == "send_email":
            if payload.get("email", "").endswith("example.com"):
                raise Exception("Simulated email failure")
            print(f" Email sent to {payload['email']}")

        elif job_type == "export_data":
            if random.random() < 0.2:
                raise Exception("Data export failed")
            print(f" Data exported for user {payload['user_id']}")

        elif job_type == "process_payment":
            if random.random() < 0.2:
                raise Exception("Payment processing failed")
            print(f" Payment processed for user {payload['user_id']}")

        else:
            raise Exception(f"Unknown job type: {job_type}")

        log_attempt(job_id, attempt_no, "succeeded")
        update_job_status(job_id, "succeeded", attempt_no)

    except Exception as exc:
        log_attempt(job_id, attempt_no, "failed", str(exc))

        if attempt_no >= max_retries:
            update_job_status(job_id, "failed", attempt_no)
            send_to_dead_letter(job_id, payload, reason=str(exc))
        else:
            update_job_status(job_id, "queued", attempt_no)
            raise self.retry(exc=exc, countdown=5)


def send_to_dead_letter(job_id, payload, reason =None):
    with open(dead_letter_file, "a") as f:
        f.write(f"Job ID {job_id}: {json.dumps(payload)}\n")
    log_to_dlq(job_id, payload, reason)
    print(f" Moved to dead-letter queue: {payload['email']}")

