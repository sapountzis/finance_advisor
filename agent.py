from typing import Annotated, TypedDict, Literal
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from db import get_db_schema, get_db_description, execute_readonly_query
from datetime import datetime
from loguru import logger


class UserQueryType(BaseModel):
    query_type: Literal["user_data", "general_finance", "invalid_topic"]



class State(TypedDict):
    # Messages have the type "list". The `add_messages` function
    # in the annotation defines how this state key should be updated
    # (in this case, it appends messages to the list, rather than overwriting them)
    messages: Annotated[list, add_messages]
    user_id: int

class InternalState(BaseModel):
    messages: Annotated[list, add_messages]
    user_id: int
    tool_error: bool = False
    tool_answer: str = ""

class SQLGeneration(BaseModel):
    sql_query: str = Field(description="The SQL query to execute")

class SQLEvaluation(BaseModel):
    is_correct: bool
    feedback: str = ""


workflow = StateGraph(State)


llm = ChatOpenAI(model="gpt-4o")

def is_valid_topic(state):
    user_query = state["messages"][-1]
    prompt = (
        "You are a text classifier. You will be given a message from a user.\n"
        "The user is not allowed to ask unrelated questions.\n"
        "Generic greetings should be declined too.\n"
        "Classify the user message into one of the following categories:\n"
        "- user_data: questions than can be aswered by performing quieries on the following schema:\n"
        "    Table: deals\n"
        "    id (INTEGER) PRIMARY KEY\n"
        "    user_id (INTEGER) \n"
        "    deal_direction (INTEGER) \n"
        "    deal_status (INTEGER) \n"
        "    deal_time_mcs (INTEGER) \n"
        "    symbol (TEXT) \n"
        "    price (REAL) \n"
        "    requested_volume (REAL) \n"
        "    profit (REAL) \n"
        "    position_id (INTEGER) \n"
        "    filled_volume (REAL) \n"
        "    bid (REAL) \n"
        "    ask (REAL) \n"
        "    id (INTEGER) PRIMARY KEY\n"
        "    user_id (INTEGER) \n"
        "    deal_direction (INTEGER) \n"
        "    deal_status (INTEGER) \n"
        "    deal_time_mcs (INTEGER) \n"
        "    symbol (TEXT) \n"
        "    price (REAL) \n"
        "    requested_volume (REAL) \n"
        "    profit (REAL) \n"
        "    position_id (INTEGER) \n"
        "    filled_volume (REAL) \n"
        "    bid (REAL) \n"
        "    ask (REAL) \n"
        "- general_finance: if the user message is related to finance domain\n"
        "- invalid_topic: if the user message is not related to finance domain or their data in the database\n"
        "\n"
        "Consider the whoe conversation history to classify the user message."
    )
    prompt = prompt.format(user_query=user_query.content)
    history = [SystemMessage(prompt)] + state["messages"]
    # logger.info(history)
    response: UserQueryType = llm.with_structured_output(UserQueryType).invoke(history)
    return response.query_type


def invalid_topic(state: State):
    user_message = state["messages"][-1]
    prompt = (
        "You are a polite and helpful assistant.\n"
        "You are talking to a user.\n"
        "\n"
        "If user has asked a question that is not related to finance or user data in database,\n"
        "you need to respond with a polite message explaining that you can only answer questions related to finance or financial user data in the database.\n"
        "\n"
        "In case of greeting user message, you should respond with a short polite greeting."
    )
    prompt = prompt.format(user_message=user_message)
    response = llm.invoke([SystemMessage(prompt)] + state["messages"])
    return {"messages": [response]}


def general_finance(state: State):
    user_message = state["messages"][-1]
    prompt = (
        "You are a polite and helpful assistant.\n"
        "You are talking to a user.\n"
        "The user has asked a question related to finance.\n"
        "You need to respond with a polite message explaining that you can answer questions related to finance.\n"
        "You should also provide a brief answer to the user's question.\n"
        "\n"
        "User message: {user_message}"
    )
    prompt = prompt.format(user_message=user_message)
    response = llm.invoke([SystemMessage(prompt)] + state["messages"])
    return {"messages": [response]}

def sql_query(state: State) -> InternalState:
    user_message = state["messages"][-1]
    sql_generation_prompt_template = (
        "You are an SQL expert.\n"
        "You are talking to a user.\n"
        "The user has asked a question related to their data in the database.\n"
        "You need to respond with a SQL query to answer the user's question.\n"
        "You may only use data that belongs to the user's user id.\n"
        "Always include `where user_id = <user_id>` in your query.\n"
        "The tag will be replaced with the actual user id.\n"
        "\n"
        "The DB has the following schema:\n"
        "{schema}\n"
        "\n"
        "The DB has the following field descritions:\n"
        "{db_description}\n"
        "\n"
        "The current time in unix milliseconds is {current_time_ms}.\n"
        "\n"
        "Your previous response was: {previous_response}\n"
        "Feedback from evaluator: {feedback}\n"
        "Reply with the SQL query only. You should not include any explanation."
        "The query should be ready to run without any modifications exept for the <user_id> tag."
        "The SQL query must also map columns to their human readable formats according to the schema description."
        "For example the time column should be converted to a human readable format from unix milliseconds."
        "No prefixes or markdoenw syntax is allowed."
    )

    sql_evaluation_prompt_template = (
        "You are an SQL evaluation expert.\n"
        "A SQL query was generated to answer a user's question.\n"
        "You must ensure that the SQL query is correct and safe to run.\n"
        "The following conditions must be met:\n"
        "- The SQL query is read-only.\n"
        "- The SQL query contains a WHERE clause that filters data by <user_id>.\n"
        "- The SQL query answers the user's question.\n"
        "- The query must be ready to be executed without any modifications exept for the <user_id> tag.\n"
        "\n"
        "DB schema:\n"
        "{schema}\n"
        "\n"
        "The cuurent time in unix milliseconds is {current_time_ms}.\n"
    )

    db_schema = get_db_schema()
    db_description = get_db_description()
    curreent_time_ms = int(datetime.now().timestamp() * 1000)
    is_correct = False
    max_retries = 3
    retries = 0
    feedback = ""
    sql_response = ""
    messages = state["messages"]
    while not is_correct and retries < max_retries:
        sql_generation_prompt = sql_generation_prompt_template.format(
            user_message=user_message,
            previous_response=sql_response,
            feedback=feedback,
            schema=db_schema,
            db_description=db_description,
            current_time_ms=curreent_time_ms
        )

        sql_response: SQLGeneration = llm.with_structured_output(SQLGeneration).invoke([
            SystemMessage(sql_generation_prompt),
            *messages,
            HumanMessage("Generate the query")
        ])
        sql_response = sql_response.sql_query

        logger.info(f"\nGenearted SQL query: {sql_response}\n")

        sql_evaluation_prompt = sql_evaluation_prompt_template.format(
            sql_query=sql_response,
            user_message=user_message,
            schema=db_schema,
            current_time_ms=curreent_time_ms
        )
        evaluation: SQLEvaluation = llm.with_structured_output(SQLEvaluation).invoke([
            SystemMessage(sql_evaluation_prompt),
            *messages,
            HumanMessage(f"Evaluate the SQL query: `{sql_response}` agaiunst the user's question: {user_message}")
        ])
        logger.info(f"\nEvaluation: {evaluation}\n")
        if evaluation.is_correct:
            try:
                result = execute_readonly_query(sql_response.replace("<user_id>", str(state["user_id"])))
                logger.info(f"\nQuery result: {result}\n")
                tool_answer = ""
                if len(result) == 0:
                    tool_answer = "No results found."
                elif len(result) == 1:
                    for row in result:
                        for key, value in row.items():
                            tool_answer += f"{key}: {value} | "
                        tool_answer = tool_answer.rstrip(" | ")
                        tool_answer += "\n"
                else:
                    tool_answer = str(result)
                logger.info(f"\nTool answer: {tool_answer}\n")
                return {"tool_answer": tool_answer}
            except Exception as e:
                logger.error(f"\nError executing query: {e}\n")
                feedback = f"The SQL query failed to execute. Error: {e}"
                is_correct = False
        else:
            feedback = evaluation.feedback
            is_correct = evaluation.is_correct

        retries += 1

    return {"tool_error": True}

def dummy_node(state: InternalState) -> State:
    logger.info(f"Tool answer: {state.tool_answer}")
    # the answer is <answer>
    return {"messages": AIMessage(content=state.tool_answer)}


def get_graph():
    workflow.add_node("invalid_topic", invalid_topic)
    workflow.add_node("general_finance", general_finance)
    workflow.add_node("user_data", sql_query)
    workflow.add_node("dummy", dummy_node)

    workflow.add_conditional_edges(START, is_valid_topic)
    workflow.add_edge("invalid_topic", END)
    workflow.add_edge("general_finance", END)
    workflow.add_edge("user_data", "dummy")
    workflow.add_edge("dummy", END)
    graph = workflow.compile()
    return graph

