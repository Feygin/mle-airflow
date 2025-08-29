# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.models import Variable

try:
    # Airflow 2.x utility to access runtime context if not passed explicitly
    from airflow.operators.python import get_current_context
except Exception:  # fallback if not available
    def get_current_context():
        return {}

# Read credentials from Airflow Variables
# Set these in Airflow as Variables: TELEGRAM_TOKEN and TELEGRAM_CHAT_ID
TELEGRAM_TOKEN = Variable.get("TELEGRAM_TOKEN", default_var=None)
TELEGRAM_CHAT_ID = Variable.get("TELEGRAM_CHAT_ID", default_var=None)

def _hook():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        raise ValueError(
            "Telegram credentials are missing. Please set Airflow Variables "
            "'TELEGRAM_TOKEN' and 'TELEGRAM_CHAT_ID'."
        )
    return TelegramHook(token=TELEGRAM_TOKEN, chat_id=TELEGRAM_CHAT_ID)

def send_telegram_failure_message(context=None, **kwargs):
    """
    Sends a Telegram message on failure.
    Can be used as an on_failure_callback in default_args.
    Accepts Airflow context; if omitted, it will be read from the current context.
    """
    context = context or get_current_context()
    hook = _hook()

    dag_id = (context.get("dag").dag_id if context.get("dag") else "unknown_dag")
    run_id = context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "unknown")

    message = f"Исполнение DAG {dag_id} с id={run_id} завершилось неудачей!"
    hook.send_message({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    })

def send_telegram_success_message(context=None, **kwargs):
    """
    Sends a Telegram message on success.
    Use as a terminal task with TriggerRule.ALL_SUCCESS.
    Accepts Airflow context; if omitted, it will be read from the current context.
    """
    context = context or get_current_context()
    hook = _hook()

    dag_id = (context.get("dag").dag_id if context.get("dag") else "unknown_dag")
    run_id = context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else "unknown")

    message = f"Исполнение DAG {dag_id} с id={run_id} прошло успешно!"
    hook.send_message({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    })
