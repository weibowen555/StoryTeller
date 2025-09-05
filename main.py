import os
from fastapi import FastAPI, Depends
from pydantic import BaseModel
import uvicorn

from agents import Runner
from triage_agent import triage_agent  # import the configured triage agent

# Ensure API key is available for OpenAI Agents SDK (from environment)
if not os.environ.get("OPENAI_API_KEY"):
    raise EnvironmentError("Please set the OPENAI_API_KEY environment variable before running.")

# Define request model for triage endpoint
class TaskRequest(BaseModel):
    query: str

app = FastAPI(title="Multi-Agent System (Triage + DB Connector)")

@app.get("/")
async def root():
    return {"message": "Multi-agent system is running", "triage_agent": triage_agent.name}

@app.post("/triage")
async def run_triage(request: TaskRequest):
    """Endpoint that accepts a user query and returns the triage agent's response."""
    user_task = request.query
    # Run the triage agent with the given input. The agent will use tools if needed.
    result = await Runner.run(triage_agent, input=user_task)
    # Return the final output from the agent
    return {"response": str(result.final_output)}

if __name__ == "__main__":
    # Run the app with Uvicorn for local testing
    uvicorn.run(app, host="0.0.0.0", port=8000)
