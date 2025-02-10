import os
import json
import time
import requests
from sqlalchemy import create_engine, text
import azure.functions as func

# Environment Variables (Set these in Azure Function Settings)
DATABASE_URL = os.getenv("AZURE_SQL_CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GPT_ENDPOINT = "https://azure-ai-instance1.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2024-08-01-preview"

# Create SQLAlchemy Engine
engine = create_engine(DATABASE_URL)

def get_failed_logs():
    """Fetch all failed audit logs from Azure SQL"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, query, execution_time, status FROM audit_log WHERE status != 'success'"))
        logs = [{"id": row[0], "query": row[1], "execution_time": row[2], "status": row[3]} for row in result]
    return logs

def generate_rca(query, execution_time, status):
    """Call OpenAI API to generate RCA for failed queries"""
    headers = {
        "api-key": OPENAI_API_KEY,
        "Content-Type": "application/json"
    }
    payload = {
        "messages": [
            {"role": "system", "content": "You are an expert database analyst."},
            {"role": "user", "content": f"Analyze this failed SQL query: {query}, status: {status}, execution time: {execution_time}. Provide an RCA."}
        ],
        "model": "gpt-4o",
        "max_tokens": 200
    }
    
    response = requests.post(GPT_ENDPOINT, headers=headers, data=json.dumps(payload))
    return response.json().get("choices", [{}])[0].get("message", {}).get("content", "RCA not available")

def main(mytimer: func.TimerRequest) -> None:
    """Azure Function Timer Trigger"""
    logs = get_failed_logs()
    
    if not logs:
        print("[INFO] No failures detected.")
        return
    
    logs_with_rca = []
    for log in logs:
        log["rca"] = generate_rca(log["query"], log["execution_time"], log["status"])
        logs_with_rca.append(log)

    # Log RCA details
    print("[LOG] RCA Analysis for Failed Queries:")
    for log in logs_with_rca:
        print(f"[FAILURE] Query: {log['query']}\nStatus: {log['status']}\nExecution Time: {log['execution_time']}s\nRCA: {log['rca']}\n{'-'*50}")
