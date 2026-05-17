import asyncio
from traceloop.sdk.decorators import task
class UI_Utils:
    
    @staticmethod
    @task(name="processing the query in chunks - streaming")
    async def process_new_delta(new_delta, claude_message, content_ui_message, function_ui_message,stream_resp,flag):
        # Handle role
        if hasattr(new_delta, "role") and new_delta.role is not None:
            claude_message["role"] = new_delta.role or ''

        # Handle content
        if hasattr(new_delta, "content") and new_delta.content is not None:
            new_content = new_delta.content or ""
            claude_message["content"] += new_content
            if not flag:
                pass
            else:
                await content_ui_message.stream_token(new_content)

        # Handle tool calls
        if hasattr(new_delta, "tool_calls") and new_delta.tool_calls is not None:
            tool_call = new_delta.tool_calls[0]
            if hasattr(tool_call, "function"):
                # Initialize tool_calls array if it doesn't exist
                if "tool_calls" not in claude_message:
                    claude_message["tool_calls"] = []
                
                if hasattr(tool_call.function, "name") and tool_call.function.name:
                    tool_call_entry = {
                        "id": tool_call.id,
                        "type": "function",
                        "function": {
                            "name": tool_call.function.name,
                            "arguments": ""
                        }
                    }
                    claude_message["tool_calls"].append(tool_call_entry)
                
                elif hasattr(tool_call.function, "arguments"):
                    # print("new delta tool_calls function for arguments:", tool_call.function)
                    claude_message["tool_calls"][0]["function"]["arguments"] += tool_call.function.arguments or ""


        return claude_message, content_ui_message, function_ui_message,stream_resp
