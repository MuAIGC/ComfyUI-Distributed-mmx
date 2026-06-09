"""
Proxy routes for local worker access.

When ComfyUI is accessed via a cloud URL (reverse proxy), the browser cannot
reach local workers on different ports (e.g. 8189) because only the master
port is exposed. These proxy endpoints forward browser requests to local
workers through the master server.
"""
import json

import aiohttp
from aiohttp import web
import server

from ..utils.config import load_config
from ..utils.logging import debug_log
from ..utils.network import (
    build_worker_url,
    get_client_session,
    handle_api_error,
    normalize_host,
)


def _is_local_worker(worker):
    """Check if a worker is local (not remote/cloud)."""
    worker_type = str(worker.get("type", "")).lower()
    if worker_type in ("remote", "cloud"):
        return False
    if worker_type == "local":
        return True
    host = normalize_host(worker.get("host")) or ""
    return host in ("", "localhost", "127.0.0.1", "::1", "0.0.0.0")


@server.PromptServer.instance.routes.get("/distributed/proxy/{worker_id}/{path:.*}")
async def proxy_to_local_worker_get(request):
    return await proxy_to_local_worker(request)


@server.PromptServer.instance.routes.post("/distributed/proxy/{worker_id}/{path:.*}")
async def proxy_to_local_worker(request):
    """
    Proxy any request from the browser to a local worker.

    Flow: Browser → Master /distributed/proxy/{worker_id}/{path} → Worker /{path}

    This solves the problem where the browser cannot reach local workers
    directly because only the master port is exposed via the cloud URL.
    """
    worker_id = request.match_info["worker_id"]
    path = request.match_info.get("path", "")

    # Read request body
    body = None
    if request.method == "POST":
        try:
            body = await request.read()
        except Exception:
            body = None

    # Find worker in config
    config = load_config()
    worker = None
    for w in config.get("workers", []):
        if str(w.get("id")) == str(worker_id):
            worker = w
            break

    if not worker:
        return web.json_response(
            {"error": f"Worker {worker_id} not found"}, status=404
        )

    if not _is_local_worker(worker):
        return web.json_response(
            {"error": "Proxy is only for local workers"}, status=400
        )

    # Build target URL using backend utility (runs on the server, knows the real host)
    target_url = build_worker_url(worker, f"/{path}" if path else "")

    # Forward query parameters
    if request.query_string:
        target_url = f"{target_url}?{request.query_string}"

    # Forward relevant headers
    headers = {}
    content_type = request.content_type
    if content_type:
        headers["Content-Type"] = content_type

    # Make proxied request
    try:
        session = await get_client_session()
        async with session.request(
            method=request.method,
            url=target_url,
            headers=headers if headers else None,
            data=body,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as worker_response:
            response_body = await worker_response.read()

            # Forward content-type from worker response
            resp_headers = {}
            worker_ct = worker_response.content_type
            if worker_ct:
                resp_headers["Content-Type"] = worker_ct

            return web.Response(
                body=response_body,
                status=worker_response.status,
                headers=resp_headers if resp_headers else None,
            )
    except asyncio.TimeoutError:
        debug_log(f"Proxy timeout: worker={worker_id} path=/{path}")
        return web.json_response(
            {"error": "Worker request timeout"}, status=504
        )
    except Exception as e:
        debug_log(f"Proxy error: worker={worker_id} path=/{path} error={e}")
        return web.json_response(
            {"error": f"Proxy failed: {str(e)}"}, status=502
        )
