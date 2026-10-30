"""Same-origin, loopback operator console with server-held credential references."""
import asyncio
from dataclasses import dataclass
import json
import logging
from pathlib import Path
from urllib.parse import urlsplit
import uuid

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware

from runtime.action_journal import JournalError
from runtime.control_store import ControlStore, ControlError
from runtime.policy_engine import PolicyError
from runtime.failure_diagnostics import classify_failure


logger = logging.getLogger("ccs.control")
STATIC = Path(__file__).with_name("static")
COOKIE = "ccs_operator"


@dataclass(frozen=True)
class ControlConfig:
    origin: str = "http://127.0.0.1:8877"

    def __post_init__(self):
        parsed = urlsplit(self.origin)
        if (parsed.scheme not in {"http", "https"} or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
                or parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment):
            raise ValueError("This control host supports an exact loopback origin only")
        if not parsed.port:
            raise ValueError("Explicit control port required")

    @property
    def secure(self):
        return self.origin.startswith("https:")


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


async def _body(request):
    async def receive():
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > 16_384:
                raise ControlError(413, "request_too_large", "Control requests are limited to 16 KiB.")
            chunks.append(chunk)
        return b"".join(chunks)
    if request.headers.get("content-type", "").split(";")[0].lower() != "application/json":
        raise ControlError(415, "json_required", "Send a JSON control request.")
    try:
        raw = await asyncio.wait_for(receive(), timeout=5)
        data = json.loads(raw, object_pairs_hook=_strict_object, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if type(data) is not dict:
            raise ValueError()
        return data
    except (ValueError, UnicodeDecodeError, RecursionError):
        raise ControlError(400, "invalid_json", "The JSON control request is invalid.") from None
    except asyncio.TimeoutError:
        raise ControlError(408, "request_timeout", "The control request body timed out.") from None


def create_app(journal, config=None):
    config = config or ControlConfig()
    store = ControlStore(journal)
    login_failures = []

    async def page(request):
        return Response((STATIC / "index.html").read_bytes(), media_type="text/html")

    async def asset(request):
        name = request.path_params["name"]
        types = {"console.js": "text/javascript", "console.css": "text/css", "mark.svg": "image/svg+xml"}
        if name not in types:
            return Response(status_code=404)
        return Response((STATIC / name).read_bytes(), media_type=types[name])

    async def login(request):
        now = journal._now()
        login_failures[:] = [value for value in login_failures if now - value < 60_000]
        if len(login_failures) >= 5:
            raise ControlError(429, "login_limited", "Too many connection attempts. Wait one minute.")
        login_failures.append(now)  # reserve the attempt before asynchronous work
        data = await _body(request)
        if set(data) != {"credential"}:
            raise ControlError(400, "invalid_request", "A provisioned operator credential is required.")
        try:
            token, identity = await run_in_threadpool(store.login, data["credential"], request.cookies.get(COOKIE))
        except PolicyError:
            raise ControlError(401, "authentication_failed", "The operator credential is invalid or lacks console access.") from None
        data.clear()
        response = JSONResponse(identity)
        response.set_cookie(COOKIE, token, httponly=True, secure=config.secure, samesite="strict",
                            max_age=max(1, (identity["expires_ms"] - identity["server_time_ms"]) // 1000), path="/")
        return response

    async def session(request):
        return JSONResponse(await run_in_threadpool(store.session, request.cookies.get(COOKIE)))

    async def logout(request):
        csrf = _csrf_header(request)
        await run_in_threadpool(store.logout, request.cookies.get(COOKIE), csrf)
        response = Response(status_code=204)
        response.delete_cookie(COOKIE, httponly=True, secure=config.secure, samesite="strict", path="/")
        return response

    async def snapshot(request):
        return JSONResponse(await run_in_threadpool(store.snapshot, request.cookies.get(COOKIE)))

    async def detail(request):
        return JSONResponse(await run_in_threadpool(store.detail, request.cookies.get(COOKIE), request.path_params["kind"], request.path_params["record_id"]))

    async def command(request):
        csrf = _csrf_header(request)
        data = await _body(request)
        result = await run_in_threadpool(store.command, request.cookies.get(COOKIE), csrf,
                                        request.path_params["command"].upper(), request.path_params["target"], data)
        return JSONResponse(result, status_code=202 if result["state"] == "REQUESTED" else 200)

    app = Starlette(routes=[Route("/", page), Route("/assets/{name}", asset),
                            Route("/api/login", login, methods=["POST"]), Route("/api/session", session),
                            Route("/api/logout", logout, methods=["POST"]), Route("/api/snapshot", snapshot),
                            Route("/api/records/{kind:record_kind}/{record_id}", detail),
                            Route("/api/commands/{command:control_kind}/{target}", command, methods=["POST"])])
    app.state.store, app.state.config = store, config

    async def boundary(request, call_next):
        request_id = uuid.uuid4().hex
        try:
            if request.headers.get("host") != urlsplit(config.origin).netloc or request.url.scheme != urlsplit(config.origin).scheme:
                raise ControlError(400, "host_rejected", "Use the configured console origin.")
            origin = request.headers.get("origin")
            if request.url.path.startswith("/api/"):
                if request.method not in {"GET", "HEAD"} and origin != config.origin:
                    raise ControlError(403, "origin_rejected", "A same-origin control request is required.")
                if origin is not None and origin != config.origin or request.headers.get("sec-fetch-site") == "cross-site":
                    raise ControlError(403, "origin_rejected", "Cross-origin console access is denied.")
            response = await call_next(request)
        except ControlError as exc:
            response = JSONResponse({"error": {"code": exc.code, "message": exc.message, "request_id": request_id}}, status_code=exc.status)
        except PolicyError:
            response = JSONResponse({"error": {"code": "authority_denied", "message": "Current authority does not permit this operation.", "request_id": request_id}}, status_code=403)
        except JournalError as exc:
            logger.error("Console journal unavailable; request=%s category=%s", request_id, classify_failure(exc))
            response = JSONResponse({"error": {"code": "journal_unavailable", "message": "Journal unavailable. Controls are disabled until a fresh read succeeds.", "request_id": request_id}}, status_code=503)
        except Exception as exc:
            logger.error("Console request failed; request=%s category=%s", request_id, classify_failure(exc))
            response = JSONResponse({"error": {"code": "server_error", "message": "The request could not be completed. Refresh journal evidence before another control request.", "request_id": request_id}}, status_code=500)
        response.headers.update({"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
                                 "Referrer-Policy": "no-referrer", "X-Frame-Options": "DENY",
                                 "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
                                 "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
                                 "X-Request-ID": request_id})
        return response
    app.add_middleware(BaseHTTPMiddleware, dispatch=boundary)
    return app


def _csrf_header(request):
    value = request.headers.get("x-ccs-csrf")
    if not value:
        raise ControlError(403, "csrf_required", "Refresh the operator session before a control request.")
    return value


# Restrict endpoint names at routing, including encoded path traversal attempts.
from starlette.convertors import Convertor, register_url_convertor


class RecordKind(Convertor):
    regex = "actions|tasks"

    def convert(self, value):
        return value


class ControlKind(RecordKind):
    regex = "approve|revoke|cancel|reconcile"


register_url_convertor("record_kind", RecordKind())
register_url_convertor("control_kind", ControlKind())
