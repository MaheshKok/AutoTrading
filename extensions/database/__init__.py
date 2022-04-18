import os

import extensions


def register_db(app):
    with app.app_context():
        # Initialize SQLAlchemy
        # app.config[
        #     "SQLALCHEMY_DATABASE_URI"
        # ] = "postgres+psycopg2://postgres:password@localhost:54322/paper_trading"
        app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
        app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"pool_recycle": 1800}
        extensions.db.init_app(app)
        extensions.db.create_all()
