import json
import os
from typing import Any

class DBT_logs_parser:
    FLAGS_TO_REPORT: list[str] = ['warn', 'error']
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LOG_FILE_PATH: str = os.getenv(
        'RUN_RESULTS_PATH', 
        os.path.join(BASE_DIR, 'dbt_analytics/target/run_results.json')
    )

    print(f"DEBUG: Looking for log file at: {LOG_FILE_PATH}")

    @staticmethod
    def get_data_from_json() -> Any:
        with open(DBT_logs_parser.LOG_FILE_PATH) as file:
            return json.load(file)

    @staticmethod
    def form_failed_task_message(data: dict) -> str:

        # form dictionary with necessary data
        task_data: dict[str, str] = {
            'status': data.get('status', 'Unknown status'),
            'message': data.get('message', 'Unknown message'),
            'failures': data.get('failures', 'Unknown'),
            'unique_id': data.get('unique_id', 'Unknown UID'),
        }

        # return formatted string
        return f"❗{task_data['status'].upper()}: {task_data['unique_id']}\nRows failed: {task_data['failures']}\nDetails: {task_data['message']}\n"

    @staticmethod
    def parse_critical_notifications() -> str:
        
        # get last run log data as python data type
        last_run_log = DBT_logs_parser.get_data_from_json()

        # check if 'results' field is in log file
        if not 'results' in last_run_log:
            print('There is an error in last run log file structure and results cannot be accessed.')
            return ""
        
        # go through every executed task and form message for flaged to report ones
        result: str = ''

        for task in last_run_log.get('results', []):

            # form message if task was flaged to be reported
            if task.get('status') in DBT_logs_parser.FLAGS_TO_REPORT:
                result += DBT_logs_parser.form_failed_task_message(data=task)
        
        return result
