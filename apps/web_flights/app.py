import os

# Default to full navigation in the flights web app.
# Set APP_PRODUCT=flights to hide non-flight pages.
os.environ.setdefault("APP_PRODUCT", "all")

from apps.web_portal.app import app  # noqa: E402

__all__ = ["app"]
