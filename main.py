from dotenv import load_dotenv
from db import init_and_populate_db
from agent import get_graph
from langchain_core.messages import HumanMessage


if __name__ == "__main__":
    load_dotenv()
    init_and_populate_db()
    graph = get_graph()
    messages = []
    while True:
        user_input = input("User: ")
        if user_input.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        messages.append(HumanMessage(content=user_input))
        state = graph.invoke({"messages": messages, "user_id": 7})
        messages = state["messages"]
        print("Assistant:", state["messages"][-1].content)