from utils.utils import current_es_config
from utils.header_decryption import get_es_config_from_headers


def extract_and_set_es_server(ctx) -> None:
    if not ctx:
        return
    try:
        if hasattr(ctx, 'request_context') and ctx.request_context:
            request = ctx.request_context.request
            if request and hasattr(request, 'headers'):
                headers_dict = dict(request.headers)
                es_config = get_es_config_from_headers(headers_dict)
                if es_config:
                    current_es_config.set(es_config)
    except Exception:
        pass
