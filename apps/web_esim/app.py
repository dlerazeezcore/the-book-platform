import os

os.environ.setdefault("APP_PRODUCT", "esim")

from apps.web_portal.app import app  # noqa: E402

__all__ = ["app"]
