# agents/initialize.py

import yaml
from openai import Agent
from db_tools import connect_database, get_database_schema

def load_agent_from_yaml(yaml_path):
    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f)
    tool_map = {
        "connect_database": connect_database,
        "get_database_schema": get_database_schema,
    }
    tools = [tool_map[tool["name"]] for tool in config.get("tools", [])]
    agent = Agent(
        name=config["name"],
        instructions=config["prompt"],
        tools=tools,
        model=config.get("model", "gpt-4.1"),
        description=config.get("description", "")
    )
    return agent
