"""include/ai_explainer_nammamart.py

Purpose:
    LangGraph agent that reads the NammaMart ETL quarantine results
    and generates:
      1. A plain-English root cause explanation
      2. A stakeholder-friendly Slack alert message

    Adapted from the teacher's ai_explainer.py to work with the
    NammaMart DQ result format (quality_checks + business_rules).

Credentials (via Airflow Variables):
    AZURE_OPENAI_API_KEY
    AZURE_OPENAI_ENDPOINT
    AZURE_OPENAI_DEPLOYMENT

    Falls back to a rule-based template if keys are not set.
"""
import json
import logging
from typing import TypedDict

from langgraph.graph import StateGraph, END

logger = logging.getLogger(__name__)


# -- State shared across LangGraph nodes -----------------------------------

class AlertState(TypedDict):
    quarantine_summary: dict
    dq_summary: dict
    total_quarantined: int
    total_passed: int
    run_id: str
    root_cause: str
    slack_message: str


def _get_llm(temperature: float = 0):
    """Returns Azure OpenAI LLM if credentials are available, else None."""
    try:
        from airflow.models import Variable
        api_key = Variable.get("AZURE_OPENAI_API_KEY", default_var=None)
        endpoint = Variable.get("AZURE_OPENAI_ENDPOINT", default_var=None)
        deployment = Variable.get(
            "AZURE_OPENAI_DEPLOYMENT", default_var="gpt-4o-mini"
        )
    except Exception:
        api_key = endpoint = deployment = None

    if not api_key or not endpoint:
        return None

    from langchain_openai import AzureChatOpenAI
    return AzureChatOpenAI(
        azure_deployment=deployment,
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version="2024-08-01-preview",
        temperature=temperature,
    )


# -- Node 1: Analyse root cause --------------------------------------------

def analyse_node(state: AlertState) -> AlertState:
    quarantine_summary = state["quarantine_summary"]
    dq_summary = state["dq_summary"]
    total_quarantined = state["total_quarantined"]
    run_id = state["run_id"]

    llm = _get_llm(temperature=0)

    if llm:
        from langchain_core.messages import HumanMessage
        prompt = f"""
You are a data quality analyst reviewing a NammaMart pipeline run.

Run ID: {run_id}
Total quarantined rows: {total_quarantined}

Quarantine breakdown by dataset:
{json.dumps(quarantine_summary, indent=2)}

DQ findings summary by dimension:
{json.dumps(dq_summary, indent=2)}

Write a short root cause analysis (3-5 sentences) explaining:
- Which datasets have the most issues
- What types of issues are most common
- Likely business impact
Keep it factual and non-technical.
"""
        response = llm.invoke([HumanMessage(content=prompt)])
        root_cause = response.content
    else:
        # -- Fallback: rule-based template ---
        lines = []
        for dataset, info in quarantine_summary.items():
            count = info.get("failed_rows", 0) if isinstance(info, dict) else 0
            if count > 0:
                lines.append(f"  {dataset}: {count} rows quarantined")

        for dimension, count in dq_summary.items():
            if count > 0:
                lines.append(f"  {dimension}: {count} findings")

        detail = "\n".join(lines) if lines else "  No specific issues recorded."

        root_cause = (
            f"NammaMart ETL run {run_id}: "
            f"{total_quarantined} rows quarantined across datasets.\n"
            f"{detail}\n"
            "These rows have been saved to quarantine files and "
            "excluded from the warehouse load."
        )

    state["root_cause"] = root_cause
    return state


# -- Node 2: Compose Slack message -----------------------------------------

def compose_node(state: AlertState) -> AlertState:
    total_quarantined = state["total_quarantined"]
    total_passed = state["total_passed"]
    root_cause = state["root_cause"]
    run_id = state["run_id"]

    llm = _get_llm(temperature=0.3)

    if llm:
        from langchain_core.messages import HumanMessage
        prompt = f"""
Convert this NammaMart data quality report into a friendly Slack alert.
Keep it under 6 lines. Use emojis. Start with a status emoji.

Run ID: {run_id}
Root cause: {root_cause}
Passed rows: {total_passed}
Quarantined rows: {total_quarantined}
"""
        response = llm.invoke([HumanMessage(content=prompt)])
        slack_message = response.content
    else:
        if total_quarantined == 0:
            slack_message = (
                f"NammaMart ETL [{run_id}]\n"
                f"All {total_passed} rows passed validation.\n"
                "No rows quarantined. Warehouse loaded successfully."
            )
        else:
            slack_message = (
                f"NammaMart ETL Data Quality Alert\n"
                f"Run: {run_id}\n"
                f"Clean rows loaded: {total_passed}\n"
                f"Rows quarantined: {total_quarantined}\n"
                f"Details:\n{root_cause}\n"
                "Action: Review quarantine files and investigate."
            )

    state["slack_message"] = slack_message
    return state


# -- Build the LangGraph ---------------------------------------------------

def _build_graph():
    graph = StateGraph(AlertState)
    graph.add_node("analyse", analyse_node)
    graph.add_node("compose", compose_node)
    graph.set_entry_point("analyse")
    graph.add_edge("analyse", "compose")
    graph.add_edge("compose", END)
    return graph.compile()


# -- Public function called from Airflow task ------------------------------

def explain_failures(quarantine_result: dict, dq_result: dict) -> str:
    """
    Generate a human-readable explanation of quarantine results.

    Args:
        quarantine_result: dict from quarantine_failed_records task
            Keys: run_id, directory, summary (dict of dataset->failed_rows)
        dq_result: dict from validate_data_quality task
            Keys: findings (list), blocking_count, warning_count

    Returns:
        Slack-ready message string. Empty string if no quarantined rows.
    """
    summary = quarantine_result.get("summary", {})
    total_quarantined = sum(
        (v.get("failed_rows", 0) if isinstance(v, dict) else 0)
        for v in summary.values()
    )

    if total_quarantined == 0:
        logger.info("[AI] No quarantined rows. Skipping explanation.")
        return ""

    # Aggregate DQ findings by dimension
    dq_summary = {}
    for finding in dq_result.get("findings", []):
        dim = finding.get("quality_dimension", "Unknown")
        dq_summary[dim] = dq_summary.get(dim, 0) + 1

    # Count passed rows from the loaded count or estimate
    total_passed = dq_result.get("total_rows", 0) - total_quarantined

    agent = _build_graph()
    final_state = agent.invoke({
        "quarantine_summary": summary,
        "dq_summary": dq_summary,
        "total_quarantined": total_quarantined,
        "total_passed": max(total_passed, 0),
        "run_id": quarantine_result.get("run_id", "unknown"),
        "root_cause": "",
        "slack_message": "",
    })

    logger.info("[AI] Root cause:\n%s", final_state["root_cause"])
    logger.info("[AI] Slack message:\n%s", final_state["slack_message"])

    return final_state["slack_message"]
