

import asyncio
import time
from datetime import datetime
import chainlit as cl
from langchain_experimental.tools.python.tool import PythonAstREPLTool 
from gilead_loader import GileadDataLoader  # Import Gilead Loader

gilead_loader = GileadDataLoader()

class PythonExecutionManager:
    def __init__(self, gilead_loader):
        """
        Initializes the execution manager with a loader.
        Args:
            gilead_loader (GileadDataLoader): The loader for Gilead-related scripts.
        """
        self.gilead_loader = gilead_loader
        self.python_repl = PythonAstREPLTool()  # Initialize the REPL tool

        # Load loading_code.py
        self.loading_code_gilead = self.gilead_loader.load_text_file(
            self.gilead_loader.INPUT_FILES_PATH + "loading_code.py"
        )

        print("Loaded loading_code_gilead successfully.")

    def repl_tool_fun(self, code):
        """
        Executes Python code using PythonAstREPLTool and measures execution time.
        Args:
            code (str): The Python code to execute.

        Returns:
            str: The output of the executed code.
        """
        print("Executing Code in REPL:", repr(code))
        before_time = datetime.now()
        
        try:
            output = self.python_repl.run(code)  # Execute using Langchain's Python REPL tool
        except Exception as e:
            output = f"Execution Error: {e}"
        
        after_time = datetime.now()
        time_difference = after_time - before_time
        print(f"Execution Time: {time_difference}")

        return str(output)

    async def run_loading_code(self, timeout=300):
        """
        Executes predefined code blocks asynchronously.
        Handles any timeout exceptions.
        Args:
            timeout (int): Maximum execution time. Default is 300 seconds.
        """
        print("Running loading_code.py...")

        if not self.loading_code_gilead:
            print("⚠No code found in loading_code.py")
            return

        try:
            function_response = await asyncio.wait_for(
                asyncio.to_thread(self.repl_tool_fun, self.loading_code_gilead),
                timeout=timeout
            )
            print("Execution Output:", function_response)
        except asyncio.TimeoutError:
            print("Execution Timed Out!")
        except Exception as e:
            print(f"Error: {e}")

    async def run_python_execution(self, code, timeout=300):
        """
        Executes arbitrary Python code asynchronously.
        Args:
            code (str): The Python code to execute.
            timeout (int): Maximum execution time.

        Returns:
            str: Execution output.
        """
        print("⚡ Executing User Code...")
        print("in executing python code!!!!!")
        function_response = await cl.make_async(self.repl_tool_fun)(code)
        return function_response