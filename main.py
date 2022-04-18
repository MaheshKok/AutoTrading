# -*- coding: utf-8 -*-
import logging

import telegram
from dotenv import find_dotenv, load_dotenv

from app import create_webapp

load_dotenv(find_dotenv())

app = None

if not app:
    app = create_webapp()
    telegram_bot = telegram.Bot(token='5123178744:AAGLvri8Td5RJec4NdP5MQn2ZUKwKlKxsvs')

if __name__ == "__main__":
    # Start application
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.run()
