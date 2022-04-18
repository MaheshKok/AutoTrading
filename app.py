
from flask import Flask

from extensions import register_extensions
from extensions.schedular.initialize import register_scheduler
from views import register_base_routes
from views import register_json_routes
from flask_cors import CORS
import sentry_sdk


def _create_app():
    """Base Flask app factory used by all apps."""
    # Create the Flask application
    app = Flask(__name__, instance_relative_config=False)
    CORS(app)
    register_extensions(app)
    register_scheduler(app)
    return app


def create_webapp() -> Flask:  # pragma: no cover
    """
    Create a version of the app suitable for serving the website locally or in production.
    """
    app = _create_app()
    register_json_routes(app)
    register_base_routes(app)

    sentry_sdk.init(
        "https://ce37badd7d894b97a19dc645745e0730@o1202314.ingest.sentry.io/6327385",

        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0,
        environment="production"
    )
    return app
