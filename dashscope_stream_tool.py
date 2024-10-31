import asyncio
import json
import os

from dashscope import Generation


# tools definition
def get_weather(location: str) -> str:
    """Simulation"""
    return f" {location}, sunny, 25 degree,"


def merge_fun_dict(dict1, dict2):
    """
    Recursively merge two function-related dictionaries with special rules.

    Args:
        dict1: Base dictionary (can be None or empty)
        dict2: Dictionary to merge into dict1

    Returns:
        dict: Merged dictionary with nested structures
    """
    # If dict1 is empty or None, return a copy of dict2
    if not dict1:
        return dict2.copy()

    # Create a copy of dict1 as the base for merging
    result = dict1.copy()

    for key, value in dict2.items():
        # Special case: if type='function', use new value directly
        if key == 'type' and value == 'function':
            result[key] = value
            continue

        # For nested dictionaries, merge recursively
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_fun_dict(result[key], value)
        # If both values are strings, concatenate them
        elif key in result and isinstance(result[key], str) and isinstance(value, str):
            # Concatenate strings, avoid duplicate concatenation
            result[key] = result[key] + value if result[key] != value else result[key]
        else:
            # For other cases (different types or new keys), use the new value
            result[key] = value

    return result


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the whether for ",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "City Name"
                    }
                },
                "required": ["location"]
            }
        }
    }
]


class ChatBot:
    def __init__(self):
        self.api_key = os.getenv('DASHSCOPE_API_KEY')
        self.messages = []
        self.tools = TOOLS

    async def stream_chat(self):
        """streaming response"""
        response = Generation.call(
            model='qwen-turbo',
            messages=self.messages,
            tools=self.tools,
            result_format='message',
            stream=True,
            incremental_output=True,
            api_key=self.api_key
        )

        tool_function = None
        for chunk in response:
            if chunk.status_code == 200:
                # in case it is a message response
                if not chunk.output.choices[0].message.get('tool_calls'):
                    yield chunk.output.choices[0].message['content']
                else:
                    # pay attention here, accumulate to invoke tool function
                    current_fun_dict = chunk.output.choices[0].message.get('tool_calls')[0]
                    tool_function = merge_fun_dict(tool_function, current_fun_dict)
            else:
                print(f"Error chunk: {chunk}")

        # tool calls
        if tool_function:
            # reconstruct the full tool message, just simplified handle one call
            message = {"role": "assistant", "tool_calls": [tool_function]}
            self.messages.append(message)
            function_name = tool_function['function']['name']
            arguments = json.loads(tool_function['function']['arguments'])

            if function_name == 'get_weather':
                result = get_weather(arguments['location'])

            tool_response = {
                "role": "tool",
                "name": function_name,
                "content": result
            }
            self.messages.append(tool_response)
            async for c in self.stream_chat():
                print(c, end="", flush=True)


async def main():
    bot = ChatBot()

    test_inputs = [
        "How is the weather of Beijing ? And explain what games  will be  popular?",
    ]

    for user_input in test_inputs:
        print(f"\nUser: {user_input}")
        print("Assistant: ", end="", flush=True)

        bot.messages.append({"role": "user", "content": user_input})
        async for chunk in bot.stream_chat():
            print(chunk, end="", flush=True)
        print()


if __name__ == "__main__":
    asyncio.run(main())
