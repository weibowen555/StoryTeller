import asyncio
from agent_init import load_workflow_agent

async def main():
    agent = load_workflow_agent("workflows/database_connect.yaml")
    print("==== MCP DatabaseConnectWorkflow Test ====")
    result = await agent.run("Test database connectivity now.")
    print("=== Agent Final Output ===")
    print(result.final_output)

if __name__ == "__main__":
    asyncio.run(main())
