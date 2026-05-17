class MessageProcessor:
    """
    A class to process incoming delta messages and update the state of the conversation.
    """

    async def process_new_delta(self, new_delta, anthropic_message, content_ui_message, function_ui_message, stop_reason):
        """
        Processes an incoming delta message to update the state of the conversation and user interface.
        
        Args:
            new_delta: The incoming delta message containing updates (e.g., message start, content updates, tool usage).
            anthropic_message: A dictionary tracking the current state of the conversation.
            content_ui_message: The UI message object for displaying content.
            function_ui_message: The UI message object for displaying function-related content.
            stop_reason (str): A variable to capture the reason for stopping the message, if any.

        Returns:
            tuple: Updated `anthropic_message`, `content_ui_message`, `function_ui_message`, and `stop_reason`.
        """
        
        if "messageStart" in new_delta:
            # Update the role in the `anthropic_message`
            anthropic_message["role"] = new_delta["messageStart"]["role"]

        elif "contentBlockDelta" in new_delta:
            new_content = new_delta["contentBlockDelta"]["delta"]

            if "text" in new_content:
                new_content_text = new_content["text"]

                if len(anthropic_message["content"]) == 0:
                    anthropic_message["content"] = []
                    anthropic_message["content"].append({"text": ""})
                anthropic_message["content"][0]["text"] += new_content_text

                # Stream the new text to the UI
                await content_ui_message.stream_token(new_content_text)

            # If the delta contains tool usage information
            elif "toolUse" in new_content:    
                for content_index in range(len(anthropic_message["content"])):
                    # Find the relevant tool usage entry
                    if "toolUse" in anthropic_message["content"][content_index]:
                        if "input" not in anthropic_message["content"][content_index]["toolUse"]:
                            anthropic_message["content"][content_index]["toolUse"]["input"] = ""
                        anthropic_message["content"][content_index]["toolUse"]["input"] += new_content["toolUse"]["input"]
                        break
            
        elif "contentBlockStart" in new_delta:
            tool = new_delta["contentBlockStart"]["start"]["toolUse"]
            if len(anthropic_message["content"]) == 0:
                anthropic_message["content"] = []
            anthropic_message["content"].append({"toolUse": {"toolUseId": tool['toolUseId'], "name": tool['name']}})
            await content_ui_message.send()
        
        elif "messageStop" in new_delta:
            stop_reason = new_delta["messageStop"]["stopReason"]

        return anthropic_message, content_ui_message, function_ui_message, stop_reason
