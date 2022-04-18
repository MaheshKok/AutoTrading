from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from extensions import database

db = SQLAlchemy()


def register_extensions(app: Flask):
    database.register_db(app)
