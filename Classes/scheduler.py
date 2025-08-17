from Classes.db import get_connection, log_job
from Classes.tasks import send_email_task

def schedule_jobs():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, name, email FROM [dbo].[users] WHERE is_active = 0")

    for user_id, name, email in cursor.fetchall():
        payload = {"user_id": str(user_id), "name": name, "email": email}
        job_id = log_job("send_email", payload, priority=5, max_retries=3)
        send_email_task.apply_async(
            args=[job_id, payload, 3],
            priority=5
        )
        print(f"Scheduled job for {email}")

    conn.close()

if __name__ == "__main__":
    schedule_jobs()
