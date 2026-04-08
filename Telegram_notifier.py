import requests
import os
from dotenv import load_dotenv

load_dotenv()


class Telegram_notifier:
    
    def __init__(self) -> None:
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('CHAT_ID')

    def send_telegram_message(self, text: str) -> None:
        data = {'chat_id': self.chat_id, 'text': text}

        if not self.token:
            print("Error: TELEGRAM_BOT_TOKEN environment variable is not set")
            return

        bot_url: str = f'https://api.telegram.org/bot{self.token}/sendMessage'
        response = requests.post(bot_url, json = data)

        if response.status_code != 200:
            print('Telegram message sending error.')
