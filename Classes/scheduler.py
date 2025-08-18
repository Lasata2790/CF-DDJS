from Classes.db import get_connection, log_job
from Classes.tasks import execute_job_task

def get_export_jobs():
    # Example: 
    return [{"user_id": "U101", "data_id": "D1001"}, {"user_id": "U102", "data_id": "D1002"}]

def get_pending_payments():
    # Example: 
    return [{"user_id": "U101", "amount": 100}, {"user_id": "U102", "amount": 150}]


def schedule_jobs():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Schedule Email Jobs (considered scenario: inactive user)
    cursor.execute("SELECT user_id, name, email FROM [dbo].[users] WHERE is_active = 0")
    for user_id, name, email in cursor.fetchall():
        payload_email = {"user_id": str(user_id), "name": name, "email": email}
        job_id_email = log_job("send_email", payload_email, priority=5, max_retries=3)
        execute_job_task.apply_async(
            args=[job_id_email, "send_email", payload_email, 3],
            priority=5
        )
        print(f"Scheduled Email job for {email}")

    # 2. Schedule Export Data Jobs
    export_jobs = get_export_jobs()  
    for job in export_jobs:
        payload_export = {"user_id": job['user_id'], "data_id": job['data_id']}
        job_id_export = log_job("export_data", payload_export, priority=3, max_retries=3)
        execute_job_task.apply_async(
            args=[job_id_export, "export_data", payload_export, 3],
            priority=3
        )
        print(f"Scheduled Export job for user {job['user_id']}")

    # 3️. Schedule Payment Jobs
    pending_payments = get_pending_payments()  
    for payment in pending_payments:
        payload_payment = {"user_id": payment['user_id'], "amount": payment['amount']}
        job_id_payment = log_job("process_payment", payload_payment, priority=2, max_retries=3)
        execute_job_task.apply_async(
            args=[job_id_payment, "process_payment", payload_payment, 3],
            priority=2
        )
        print(f"Scheduled Payment job for user {payment['user_id']}")

    conn.close()

if __name__ == "__main__":
    schedule_jobs()
