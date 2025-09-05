import yaml
from agents import Agent


def load_workflow_agent(
    yaml_path: str,
    mcp_server,
) -> Agent:
    """
    Load a workflow agent from a YAML workflow definition, with all MCP tools auto-registered.
    Args:
        yaml_path: Path to the workflow YAML file.
        mcp_server: The instantiated MCP server object (e.g., your DatabaseMCPServer).
    Returns:
        Agent: Configured Agent instance.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    agent = Agent(
        name=config.get("name", "WorkflowAgent"),
        instructions=config["prompt"],
        model=config.get("model", "gpt-4.1"),
        description=config.get("description", ""),
        # Register MCP server; tools will be listed/called via MCP automatically
        mcp_servers=[mcp_server],
    )
    return agent