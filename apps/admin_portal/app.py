import os

os.environ.setdefault("APP_PRODUCT", "admin")

from apps.web_portal.app import app  # noqa: E402

__all__ = ["app"]
