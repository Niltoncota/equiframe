import os
from celery import Celery

broker = os.getenv("REDIS_URL", "redis://redis:6379/0")
backend = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = Celery("equiframe", broker=broker, backend=backend)

app.conf.broker_connection_retry_on_startup = True

# Registra tasks externas automaticamente no boot
app.conf.imports = ("app.pipeline.tasks",)
# (Opcional) manter a fila padrão alinhada ao worker:
app.conf.task_default_queue = "celery"

app.conf.beat_schedule = {
    "process-batch-hourly": {
        "task": "app.pipeline.tasks.process_batch",
        "schedule": 3600.0,
    }
}

# Exemplo de tarefa rápida para teste
@app.task
def ping():
    return "pong"

