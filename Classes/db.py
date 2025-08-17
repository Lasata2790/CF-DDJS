import pyodbc
import json

SERVER = "localhost\\SQLEXPRESS"
DATABASE = "DJSS"
# USERNAME = "lasul"
# PASSWORD = "4837"



def get_connection():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )
    return conn

def log_job(job_type, payload, priority=5, max_retries=3):
    
    user_id= '6E3A8F24-8F42-4D2F-BB03-39B42A27F341'
    status = 'queued'
    attempts_done = 1
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO jobs (user_id,job_type, payload, priority,status, attempts_max, attempts_done, created_at) " \
        "OUTPUT INSERTED.job_id" \
        " VALUES (?, ?, ?, ?, ? ,?,?,  SYSDATETIME()); SELECT SCOPE_IDENTITY()",
        (user_id, job_type, json.dumps(payload), priority, status, max_retries,  attempts_done)
    )
    job_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    return job_id

def log_attempt(job_id, attempt_no, status, error_message=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO job_attempts (job_id, attempt_no, status, error_message) VALUES (?, ?, ?, ?)",
        (job_id, attempt_no, status, error_message)
    )
    conn.commit()
    conn.close()

def update_job_status(job_id, status, attempts_done):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE jobs SET status=?, attempts_done=?, updated_at=SYSUTCDATETIME() WHERE job_id=?",
        (status, attempts_done, job_id)
    )
    conn.commit()
    conn.close()
