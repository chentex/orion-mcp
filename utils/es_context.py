"""Utility to extract and set ES server config from MCP request headers."""
import logging

from utils.utils import current_es_config
from utils.header_decryption import get_es_config_from_headers

logger = logging.getLogger(__name__)


def extract_and_set_es_server(ctx) -> None:
    """Extract ES config from request headers and store in context variable.

    Clears stale config at the start of each call so a failed or missing
    extraction never leaks a previous caller's ES configuration.
    Propagates ValueError from decryption failures instead of swallowing them.
    """
    current_es_config.set(None)

    if not ctx:
        return
    if not (hasattr(ctx, 'request_context') and ctx.request_context):
        return

    request = ctx.request_context.request
    if not request or not hasattr(request, 'headers'):
        return

    headers_dict = dict(request.headers)
    es_config = get_es_config_from_headers(headers_dict)
    if es_config:
        current_es_config.set(es_config)
