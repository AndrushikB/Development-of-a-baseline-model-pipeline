from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7552693381:AAGzPn18ZH8JkHk3nisUzXFzF7nhPGclMkk}',
                        chat_id='{-1002431458997}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'chat_id': '{-1002431458997}',
        'text': message
    })

def send_telegram_failure_message(context):
	# ваш код здесь #
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7552693381:AAGzPn18ZH8JkHk3nisUzXFzF7nhPGclMkk}',
                        chat_id='{-1002431458997}')
    dag = context['dag']
    task_instance_key_str = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} не удалось :( \n  DAG log: {task_instance_key_str}'
    hook.send_message({
        'chat_id': '{-1002431458997}',
        'text': message
    })