from dotenv import load_dotenv
import openai
import os
import requests
import json
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod

# Load OpenAI key from .env file
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI()

MCP_SERVER_URL = "http://localhost:8000/mcp"

def get_tools_from_mcp():
    """Fetch available tools from the MCP server"""
    try:
        response = requests.post(MCP_SERVER_URL, json={"method": "tools/list"})
        tools = response.json().get("tools", [])
        openai_tools = []
        
        for tool in tools:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["parameters"]
                }
            })
        return openai_tools
    except Exception as e:
        print(f"Error fetching tools from MCP server: {e}")
        return []

def mcp_tool_call(tool_name, arguments):
    """Execute a tool call on the MCP server"""
    print(f"🔧 Calling MCP tool: {tool_name} with arguments: {arguments}")
    
    payload = {
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments}
    }
    
    try:
        response = requests.post(MCP_SERVER_URL, json=payload)
        data = response.json()
        
        if "content" in data and isinstance(data["content"], list):
            return data["content"][0].get("text", str(data))
        return str(data)
    except Exception as e:
        return f"❌ MCP tool call failed: {e}"

class BaseAgent(ABC):
    """Base class for all agents"""
    def __init__(self, name: str, description: str, model: str = "gpt-4o"):
        self.name = name
        self.description = description
        self.model = model
        self.conversation_history = []
    
    def add_to_history(self, role: str, content: str, **kwargs):
        """Add message to conversation history"""
        message = {"role": role, "content": content}
        message.update(kwargs)
        self.conversation_history.append(message)
    
    def clear_history(self):
        """Clear conversation history"""
        self.conversation_history = []
    
    @abstractmethod
    def process(self, task: str, context: Dict[str, Any] = None) -> str:
        """Process a task and return result"""
        pass

class SubAgent(BaseAgent):
    """Sub-agent that can call MCP tools for specific domains"""
    def __init__(self, name: str, description: str, capabilities: List[str], model: str = "gpt-4o"):
        super().__init__(name, description, model)
        self.capabilities = capabilities
        self.tools = get_tools_from_mcp()
        self.system_prompt = f"""You are {name}, a specialized AI assistant.

Description: {description}

Your capabilities include: {', '.join(capabilities)}

You have access to MCP tools and should use them when appropriate to complete tasks.
Be concise and focused in your responses. Always explain what tools you're using and why."""
    
    def process(self, task: str, context: Dict[str, Any] = None) -> str:
        """Process a task using available MCP tools"""
        print(f"🤖 {self.name} processing: {task}")
        
        # Clear previous history and set system prompt
        self.clear_history()
        self.add_to_history("system", self.system_prompt)
        
        # Add context if provided
        if context:
            context_str = f"Additional context: {json.dumps(context, indent=2)}"
            self.add_to_history("user", f"{context_str}\n\nTask: {task}")
        else:
            self.add_to_history("user", task)
        
        try:
            # Initial API call with tools
            response = client.chat.completions.create(
                model=self.model,
                messages=self.conversation_history,
                tools=self.tools if self.tools else None,
                tool_choice="auto" if self.tools else None
            )
            
            message = response.choices[0].message
            
            # Handle tool calls if present
            if message.tool_calls:
                # Add assistant message with tool calls to history
                self.add_to_history("assistant", message.content, tool_calls=[
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments
                        }
                    } for tc in message.tool_calls
                ])
                
                # Execute each tool call
                for tool_call in message.tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)
                    
                    # Call MCP server
                    tool_result = mcp_tool_call(function_name, function_args)
                    
                    # Add tool result to conversation history
                    self.add_to_history("tool", tool_result, tool_call_id=tool_call.id)
                
                # Get final response after tool execution
                final_response = client.chat.completions.create(
                    model=self.model,
                    messages=self.conversation_history,
                    tools=self.tools if self.tools else None,
                    tool_choice="auto" if self.tools else None
                )
                
                final_message = final_response.choices[0].message.content
                self.add_to_history("assistant", final_message)
                return final_message
            
            else:
                # No tool calls, just return the response
                self.add_to_history("assistant", message.content)
                return message.content
                
        except Exception as e:
            error_msg = f"❌ Error in {self.name}: {e}"
            print(error_msg)
            return error_msg

class TriageAgent(BaseAgent):
    """Main triage agent that dispatches tasks to sub-agents"""
    def __init__(self, model: str = "gpt-4o"):
        super().__init__(
            name="TriageAgent", 
            description="Main coordination agent that routes tasks to specialized sub-agents",
            model=model
        )
        self.sub_agents = {}
        self.setup_system_prompt()
    
    def setup_system_prompt(self):
        """Setup the system prompt for task routing"""
        agent_descriptions = []
        for name, agent in self.sub_agents.items():
            agent_descriptions.append(f"- {name}: {agent.description} (Capabilities: {', '.join(agent.capabilities)})")
        
        agents_list = "\n".join(agent_descriptions) if agent_descriptions else "No sub-agents currently registered"
        
        self.system_prompt = f"""You are the TriageAgent, responsible for coordinating and dispatching tasks to specialized sub-agents.

Available Sub-Agents:
{agents_list}

Your job is to:
1. Analyze incoming user requests
2. Determine which sub-agent(s) are best suited for the task
3. Break down complex tasks into manageable subtasks if needed
4. Coordinate between multiple agents if required
5. Provide a final consolidated response

When routing tasks:
- Choose the most appropriate sub-agent based on the task requirements
- If multiple agents are needed, coordinate their work
- If no sub-agent is suitable, handle the task yourself
- Always explain your routing decisions

Respond with a JSON object containing:
{{
    "agent": "agent_name",
    "subtasks": ["task1", "task2", ...],
    "reasoning": "why this agent was chosen",
    "requires_coordination": false
}}

If the task doesn't require a sub-agent, respond with:
{{
    "agent": "self",
    "response": "direct response to user",
    "reasoning": "why no sub-agent was needed"
}}"""
    
    def register_sub_agent(self, agent: SubAgent):
        """Register a sub-agent with the triage system"""
        self.sub_agents[agent.name] = agent
        self.setup_system_prompt()  # Update system prompt with new agent
        print(f"✅ Registered sub-agent: {agent.name}")
    
    def process(self, task: str, context: Dict[str, Any] = None) -> str:
        """Process a task by routing to appropriate sub-agent"""
        print(f"🎯 TriageAgent analyzing task: {task}")
        
        # Clear history and set system prompt
        self.clear_history()
        self.add_to_history("system", self.system_prompt)
        self.add_to_history("user", f"Task: {task}")
        
        try:
            # Get routing decision
            response = client.chat.completions.create(
                model=self.model,
                messages=self.conversation_history,
                response_format={"type": "json_object"}
            )
            
            routing_decision = json.loads(response.choices[0].message.content)
            print(f"📋 Routing Decision: {routing_decision}")
            
            # Handle different routing scenarios
            if routing_decision.get("agent") == "self":
                return routing_decision.get("response", "I can help with that directly.")
            
            agent_name = routing_decision.get("agent")
            subtasks = routing_decision.get("subtasks", [task])
            
            if agent_name not in self.sub_agents:
                return f"❌ Sub-agent '{agent_name}' not found. Available agents: {list(self.sub_agents.keys())}"
            
            # Execute subtasks with the selected agent
            agent = self.sub_agents[agent_name]
            results = []
            
            for subtask in subtasks:
                result = agent.process(subtask, context)
                results.append(f"**{subtask}**\n{result}")
            
            # Compile final response
            final_response = f"**Task completed by {agent_name}:**\n\n" + "\n\n".join(results)
            return final_response
            
        except Exception as e:
            error_msg = f"❌ Error in TriageAgent: {e}"
            print(error_msg)
            return error_msg

def create_default_sub_agents():
    """Create a set of default sub-agents for common tasks"""
    agents = []
    
    # Data Analysis Agent
    data_agent = SubAgent(
        name="DataAnalyst",
        description="Specializes in data analysis, file processing, and statistical operations",
        capabilities=["data analysis", "file reading", "statistical calculations", "data visualization"]
    )
    agents.append(data_agent)
    
    # Web Research Agent
    web_agent = SubAgent(
        name="WebResearcher", 
        description="Handles web searches, content fetching, and online research tasks",
        capabilities=["web search", "content fetching", "information gathering", "fact checking"]
    )
    agents.append(web_agent)
    
    # File Operations Agent
    file_agent = SubAgent(
        name="FileManager",
        description="Manages file operations, document processing, and file system tasks",
        capabilities=["file operations", "document processing", "text extraction", "file organization"]
    )
    agents.append(file_agent)
    
    # Communication Agent
    comm_agent = SubAgent(
        name="Communicator",
        description="Handles email, messaging, and communication-related tasks",
        capabilities=["email management", "message composition", "communication", "notifications"]
    )
    agents.append(comm_agent)
    
    # General Assistant
    general_agent = SubAgent(
        name="GeneralAssistant",
        description="Handles general queries, calculations, and tasks not covered by other agents",
        capabilities=["general assistance", "calculations", "text processing", "reasoning"]
    )
    agents.append(general_agent)
    
    return agents

def main():
    print("=== 🎯 Triage Agent System with MCP Tools ===")
    
    # Initialize triage agent
    triage_agent = TriageAgent()
    
    # Create and register sub-agents
    sub_agents = create_default_sub_agents()
    for agent in sub_agents:
        triage_agent.register_sub_agent(agent)
    
    # Check MCP tools availability
    tools = get_tools_from_mcp()
    if not tools:
        print("⚠️  Warning: No tools loaded from MCP server. Sub-agents will work without MCP tools.")
    else:
        print(f"✅ Loaded {len(tools)} MCP tools available to sub-agents")
        for tool in tools[:3]:  # Show first 3 tools
            print(f"  - {tool['function']['name']}: {tool['function']['description']}")
        if len(tools) > 3:
            print(f"  ... and {len(tools) - 3} more tools")
    
    print(f"\n🤖 Available Sub-Agents: {list(triage_agent.sub_agents.keys())}")
    print("\nType your instruction or question (type 'exit' to quit).")
    print("Examples:")
    print("- 'Analyze the data in my spreadsheet'")
    print("- 'Search for recent news about AI'") 
    print("- 'Help me organize my files'")
    print("- 'Send an email to my team'")
    
    while True:
        try:
            user_input = input("\n🧑 You: ").strip()
            if user_input.lower() in ["exit", "quit", "q"]:
                print("Goodbye! 👋")
                break
            
            if not user_input:
                continue
            
            print("\n" + "="*50)    
            response = triage_agent.process(user_input)
            print(f"\n🎯 Final Response:\n{response}")
            print("="*50)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye! 👋")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()