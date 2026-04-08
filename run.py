from DBT_logs_parser import DBT_logs_parser
from Telegram_notifier import Telegram_notifier


if __name__ == '__main__':

    # form message
    message: str = DBT_logs_parser.parse_critical_notifications()

    # initialize notifier and send message
    notifier = Telegram_notifier()
    
    if message:
        notifier.send_telegram_message(message)