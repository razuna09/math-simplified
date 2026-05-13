import os
import uuid
import hashlib
import random
import re
import base64
import hmac
from io import BytesIO
from datetime import datetime, timezone, timedelta
import requests
import json
import threading
from urllib.parse import quote
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash, Response, stream_with_context
from werkzeug.exceptions import HTTPException
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

PIL_AVAILABLE = True
PIL_IMPORT_ERROR = None
try:
    from PIL import Image, ImageFilter, ImageOps
except Exception as exc:
    PIL_AVAILABLE = False
    PIL_IMPORT_ERROR = str(exc)
    Image = None
    ImageFilter = None
    ImageOps = None

PYTESSERACT_AVAILABLE = True
PYTESSERACT_IMPORT_ERROR = None
try:
    import pytesseract
except Exception as exc:
    PYTESSERACT_AVAILABLE = False
    PYTESSERACT_IMPORT_ERROR = str(exc)
    pytesseract = None

AZURE_AVAILABLE = True
AZURE_IMPORT_ERROR = None
try:
    from azure.data.tables import TableServiceClient
    from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
except Exception as exc:
    AZURE_AVAILABLE = False
    AZURE_IMPORT_ERROR = str(exc)

    class ResourceExistsError(Exception):
        pass

    class ResourceNotFoundError(Exception):
        pass

    TableServiceClient = None

try:
    from azure.core.pipeline.transport import RequestsTransport
except Exception:
    RequestsTransport = None

BLOB_AVAILABLE = True
BLOB_IMPORT_ERROR = None
try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
except Exception as exc:
    BLOB_AVAILABLE = False
    BLOB_IMPORT_ERROR = str(exc)
    BlobServiceClient = None
    ContentSettings = None

"""Configuration and safety helpers"""


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_STORAGE_DIR = os.path.join(BASE_DIR, "instance", "local_storage")
LOCAL_TABLES_FILE = os.path.join(LOCAL_STORAGE_DIR, "tables.json")
LOCAL_UPLOAD_DIR = os.path.join(BASE_DIR, "instance", "chat_uploads")
LOCAL_CHAT_IMAGE_CONTAINER = "__local__"
_LOCAL_TABLE_LOCK = threading.RLock()


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _load_local_env():
    env_path = os.path.join(BASE_DIR, ".env")
    if not os.path.isfile(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


def _utc_now_iso_raw() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _local_entity_key(partition_key: str, row_key: str) -> str:
    return f"{partition_key}\t{row_key}"


def _load_local_tables_data() -> dict:
    _ensure_dir(LOCAL_STORAGE_DIR)
    if not os.path.isfile(LOCAL_TABLES_FILE):
        return {"tables": {}}
    try:
        with open(LOCAL_TABLES_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("tables"), dict):
            return data
    except Exception:
        pass
    return {"tables": {}}


def _save_local_tables_data(data: dict):
    _ensure_dir(LOCAL_STORAGE_DIR)
    with open(LOCAL_TABLES_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=True)


def _match_local_filter(entity: dict, filter_expr: str) -> bool:
    expr = (filter_expr or "").strip()
    if not expr:
        return True

    def _match_clause(clause: str) -> bool:
        part = clause.strip()
        m = re.match(r"^([A-Za-z0-9_]+)\s+eq\s+'([^']*)'$", part)
        if not m:
            return False
        field, value = m.group(1), m.group(2)
        return str(entity.get(field, "")) == value

    for or_part in expr.split(" or "):
        and_parts = [p.strip() for p in or_part.split(" and ") if p.strip()]
        if and_parts and all(_match_clause(part) for part in and_parts):
            return True
    return False


class LocalTableClient:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def _table_store(self, data: dict) -> dict:
        tables = data.setdefault("tables", {})
        return tables.setdefault(self.table_name, {})

    def create_entity(self, entity: dict):
        partition_key = str(entity.get("PartitionKey") or "")
        row_key = str(entity.get("RowKey") or "")
        if not partition_key or not row_key:
            raise ValueError("PartitionKey and RowKey are required.")
        key = _local_entity_key(partition_key, row_key)
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            table = self._table_store(data)
            if key in table:
                raise ResourceExistsError("Entity already exists.")
            stored = dict(entity)
            stored["Timestamp"] = _utc_now_iso_raw()
            table[key] = stored
            _save_local_tables_data(data)
        return dict(stored)

    def upsert_entity(self, entity: dict):
        partition_key = str(entity.get("PartitionKey") or "")
        row_key = str(entity.get("RowKey") or "")
        if not partition_key or not row_key:
            raise ValueError("PartitionKey and RowKey are required.")
        key = _local_entity_key(partition_key, row_key)
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            table = self._table_store(data)
            current = dict(table.get(key) or {})
            current.update(entity or {})
            current["PartitionKey"] = partition_key
            current["RowKey"] = row_key
            current["Timestamp"] = _utc_now_iso_raw()
            table[key] = current
            _save_local_tables_data(data)
        return dict(current)

    def get_entity(self, partition_key: str, row_key: str):
        key = _local_entity_key(str(partition_key or ""), str(row_key or ""))
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            entity = self._table_store(data).get(key)
        if not entity:
            raise ResourceNotFoundError("Entity not found.")
        return dict(entity)

    def delete_entity(self, partition_key: str, row_key: str):
        key = _local_entity_key(str(partition_key or ""), str(row_key or ""))
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            table = self._table_store(data)
            if key not in table:
                raise ResourceNotFoundError("Entity not found.")
            del table[key]
            _save_local_tables_data(data)

    def list_entities(self):
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            table = self._table_store(data)
            items = [dict(ent) for ent in table.values()]
        items.sort(key=lambda ent: (str(ent.get("PartitionKey") or ""), str(ent.get("RowKey") or "")))
        return items

    def query_entities(self, filter: str = "", select=None):
        items = self.list_entities()
        filtered = [ent for ent in items if _match_local_filter(ent, filter)]
        if select:
            out = []
            for ent in filtered:
                slim = {}
                for key in select:
                    if key in ent:
                        slim[key] = ent[key]
                out.append(slim)
            return out
        return filtered


class LocalTableServiceClient:
    def create_table(self, table_name: str):
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            tables = data.setdefault("tables", {})
            if table_name in tables:
                raise ResourceExistsError("Table already exists.")
            tables[table_name] = {}
            _save_local_tables_data(data)

    def get_table_client(self, table_name: str):
        with _LOCAL_TABLE_LOCK:
            data = _load_local_tables_data()
            data.setdefault("tables", {}).setdefault(table_name, {})
            _save_local_tables_data(data)
        return LocalTableClient(table_name)


_load_local_env()


# All connection strings must be provided via environment variables. No hardcoded values.
DEFAULT_CONN_STR = os.getenv("DEFAULT_CONN_STR", "")
TEMP_CHAT_BLOB_CONN_STR = os.getenv("TEMP_CHAT_BLOB_CONN_STR", "")

#tes
def _parse_conn_str(conn_str: str) -> dict:
    parts = {}
    if not conn_str:
        return parts
    for seg in conn_str.split(";"):
        if not seg:
            continue
        if "=" in seg:
            k, v = seg.split("=", 1)
            parts[k.strip()] = v.strip()
    return parts


def _is_valid_conn_str(conn_str: str) -> bool:
    p = _parse_conn_str(conn_str)
    if not p.get("AccountName"):
        return False
    # Either a non-empty AccountKey or a non-empty SAS is required
    key = (p.get("AccountKey") or "").strip()
    sas = (p.get("SharedAccessSignature") or "").strip()
    return bool(key) or bool(sas)


def _resolve_conn_str() -> str:
    # Prefer environment variables if they look valid and non-empty
    candidates = [
        os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        os.getenv("AZURE_TABLES_CONNECTION_STRING"),
        os.getenv("AZURE_CONN_STR"),
    ]

    # Support building from individual env vars if provided
    account = os.getenv("AZURE_ACCOUNT_NAME")
    key = os.getenv("AZURE_ACCOUNT_KEY")
    sas = os.getenv("AZURE_SAS_TOKEN")
    if account and (key or sas):
        built = (
            f"DefaultEndpointsProtocol=https;AccountName={account};"
            + (f"AccountKey={key};" if key else "")
            + (f"SharedAccessSignature={sas};" if sas else "")
            + "EndpointSuffix=core.windows.net"
        )
        candidates.insert(0, built)

    for c in candidates:
        if c and _is_valid_conn_str(c):
            return c

    # As a last resort, do not use any fallback. Only environment variables are allowed.
    return DEFAULT_CONN_STR if _is_valid_conn_str(DEFAULT_CONN_STR) else ""



# Final connection string used by the app. Only environment variables are allowed for secrets and connection strings.
AZURE_CONN_STR = _resolve_conn_str()
USE_AZURE_TABLES = bool(AZURE_AVAILABLE and AZURE_CONN_STR)

SECRET_KEY = (
    os.getenv("FLASK_SECRET_KEY")
    or os.getenv("SECRET_KEY")
    or os.urandom(32).hex()
)
APP_VERSION = os.getenv("APP_VERSION", "")
ADMIN_SESSION_ID = os.getenv("ADMIN_SESSION_ID", "999000999")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")
FORMSPREE_REGISTER_ENDPOINT = os.getenv("FORMSPREE_REGISTER_ENDPOINT", "https://formspree.io/f/meevznlj").strip()
FORMSPREE_SUPPORT_ENDPOINT = os.getenv("FORMSPREE_SUPPORT_ENDPOINT", "https://formspree.io/f/mqewlnqj").strip()
# N8N webhooks
N8N_TEST_WEBHOOK = os.getenv("N8N_WEBHOOK_URL", "")
N8N_PROD_WEBHOOK = os.getenv("N8N_WEBHOOK_URL_PROD", "")
# Webhook for deleting all chat history in external DB (expects GET with ?session_id=...)
N8N_DELETE_WEBHOOK = os.getenv("N8N_DELETE_WEBHOOK_URL", "")
try:
    OCR_MAX_FILE_MB = max(1, int(os.getenv("OCR_MAX_FILE_MB", "8")))
except Exception:
    OCR_MAX_FILE_MB = 8
OCR_LANG = os.getenv("OCR_LANG", "eng").strip() or "eng"
CHAT_IMAGE_CONTAINER = os.getenv("CHAT_IMAGE_CONTAINER", "")
CHAT_IMAGE_BLOB_PREFIX = os.getenv("CHAT_IMAGE_BLOB_PREFIX", "")
CHAT_BLOB_CONN_STR = os.getenv("CHAT_BLOB_CONNECTION_STRING", "")
try:
    CHAT_IMAGE_MAX_FILE_MB = max(1, int(os.getenv("CHAT_IMAGE_MAX_FILE_MB", str(OCR_MAX_FILE_MB))))
except Exception:
    CHAT_IMAGE_MAX_FILE_MB = OCR_MAX_FILE_MB
CHAT_ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "webp", "bmp", "gif", "tif", "tiff"}


def _has_blob_storage_config() -> bool:
    container = str(CHAT_IMAGE_CONTAINER or "").strip()
    if not container:
        return False
    return bool(_is_valid_conn_str((CHAT_BLOB_CONN_STR or "").strip()) or _is_valid_conn_str(AZURE_CONN_STR))

app = Flask(__name__)
app.secret_key = SECRET_KEY

# Table names
REGISTER_TABLE = "registerchatapp"  # users
CHATS_TABLE = "chat"                # chat messages
CONFIG_TABLE = "appconfig"          # app-wide config (announcements, maintenance)
CONTACT_TABLE = "contactrequests"   # registration/support intake
CHANGEFEED_TABLE = "changefeed"     # admin-authored change feed articles

# In-memory flags/caches for snappy checks without hitting Azure on each request
INIT_DONE = False
MAINTENANCE_STATE = None  # {"active": bool, "message": str}
ANNOUNCEMENT_STATE = None # {"title": str, "message": str, "level": str, "active": bool}
WEBHOOK_ENV_STATE = 'prod'  # 'test' | 'prod'

# ------------ Azure Table helpers ------------
SVC_CLIENT = None
TABLE_CLIENTS = {}
BLOB_SVC_CLIENT = None
BLOB_CONTAINER_CLIENTS = {}

def _svc():
    if not USE_AZURE_TABLES:
        return LocalTableServiceClient()
    global SVC_CLIENT
    if SVC_CLIENT:
        return SVC_CLIENT
    transport = None
    if RequestsTransport:
        try:
            timeout = float(os.getenv("AZURE_TIMEOUT_SECONDS", "5"))
            transport = RequestsTransport(connection_timeout=timeout, read_timeout=timeout)
        except Exception:
            transport = None
    SVC_CLIENT = TableServiceClient.from_connection_string(conn_str=AZURE_CONN_STR, transport=transport)
    return SVC_CLIENT

def _table(name):
    tbl = TABLE_CLIENTS.get(name)
    if tbl:
        return tbl
    svc = _svc()
    tbl = svc.get_table_client(name)
    TABLE_CLIENTS[name] = tbl
    return tbl

def _blob_svc():
    if not BLOB_AVAILABLE:
        raise RuntimeError(f"Azure Blob SDK not available: {BLOB_IMPORT_ERROR or 'unknown import error'}")
    conn_str = _blob_conn_str()
    global BLOB_SVC_CLIENT
    if BLOB_SVC_CLIENT:
        return BLOB_SVC_CLIENT
    BLOB_SVC_CLIENT = BlobServiceClient.from_connection_string(conn_str=conn_str)
    return BLOB_SVC_CLIENT

def _blob_container(name: str):
    key = str(name or "").strip()
    if not key:
        raise RuntimeError("Blob container name is missing.")
    cached = BLOB_CONTAINER_CLIENTS.get(key)
    if cached:
        return cached
    svc = _blob_svc()
    container = svc.get_container_client(key)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    except Exception as exc:
        # Some SDK versions surface generic errors for existing containers.
        if "ContainerAlreadyExists" not in str(exc):
            raise
    BLOB_CONTAINER_CLIENTS[key] = container
    return container

def _blob_conn_str():
    conn = (CHAT_BLOB_CONN_STR or "").strip()
    if not _is_valid_conn_str(conn):
        conn = AZURE_CONN_STR
    if not _is_valid_conn_str(conn):
        raise RuntimeError("Blob connection string is missing or invalid.")
    return conn


def _local_blob_abspath(blob_name: str) -> str:
    rel = str(blob_name or "").replace("\\", "/").lstrip("/")
    norm = os.path.normpath(rel)
    if norm.startswith(".."):
        raise RuntimeError("Invalid local blob path.")
    root = os.path.abspath(LOCAL_UPLOAD_DIR)
    path = os.path.abspath(os.path.join(root, norm))
    if os.path.commonpath([root, path]) != root:
        raise RuntimeError("Invalid local blob path.")
    return path


def _store_chat_image_locally(blob_name: str, raw: bytes):
    path = _local_blob_abspath(blob_name)
    _ensure_dir(os.path.dirname(path))
    with open(path, "wb") as fh:
        fh.write(raw)
    return path


def _read_chat_image_locally(blob_name: str) -> bytes:
    path = _local_blob_abspath(blob_name)
    with open(path, "rb") as fh:
        return fh.read()

def _delete_chat_image_locally(blob_name: str):
    path = _local_blob_abspath(blob_name)
    if os.path.isfile(path):
        os.remove(path)

def _rfc1123_now():
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

def _shared_key_auth_header(account_name: str, account_key_b64: str, method: str, content_length: int, content_type: str, canonical_headers: dict, canonical_resource: str):
    header_lines = []
    for k in sorted(canonical_headers):
        if k.lower().startswith("x-ms-"):
            header_lines.append(f"{k.lower()}:{canonical_headers[k]}")
    canonicalized_headers = "\n".join(header_lines)
    if canonicalized_headers:
        canonicalized_headers += "\n"
    length_for_sign = "" if int(content_length or 0) == 0 else str(int(content_length))
    string_to_sign = (
        f"{method}\n"          # VERB
        "\n"                   # Content-Encoding
        "\n"                   # Content-Language
        f"{length_for_sign}\n" # Content-Length
        "\n"                   # Content-MD5
        f"{content_type}\n"    # Content-Type
        "\n"                   # Date
        "\n"                   # If-Modified-Since
        "\n"                   # If-Match
        "\n"                   # If-None-Match
        "\n"                   # If-Unmodified-Since
        "\n"                   # Range
        f"{canonicalized_headers}{canonical_resource}"
    )
    try:
        key = base64.b64decode(account_key_b64)
    except Exception as exc:
        raise RuntimeError(f"Invalid AccountKey in blob connection string: {str(exc)}")
    sig = base64.b64encode(hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).digest()).decode("utf-8")
    return f"SharedKey {account_name}:{sig}"

def _upload_chat_image_via_rest(conn_str: str, container_name: str, blob_name: str, raw: bytes, mime: str):
    parts = _parse_conn_str(conn_str)
    account_name = (parts.get("AccountName") or "").strip()
    account_key = (parts.get("AccountKey") or "").strip()
    endpoint_suffix = (parts.get("EndpointSuffix") or "core.windows.net").strip()
    protocol = (parts.get("DefaultEndpointsProtocol") or "https").strip()
    if not account_name or not account_key:
        raise RuntimeError("Blob REST fallback requires AccountName and AccountKey.")

    encoded_blob_name = quote(blob_name, safe="/-_.~")
    url = f"{protocol}://{account_name}.blob.{endpoint_suffix}/{container_name}/{encoded_blob_name}"
    x_ms_date = _rfc1123_now()
    x_ms_version = "2021-12-02"
    headers = {
        "x-ms-blob-type": "BlockBlob",
        "x-ms-date": x_ms_date,
        "x-ms-version": x_ms_version,
        "Content-Type": mime or "application/octet-stream",
        "Content-Length": str(len(raw)),
    }
    canonical_resource = f"/{account_name}/{container_name}/{blob_name}"
    auth = _shared_key_auth_header(
        account_name=account_name,
        account_key_b64=account_key,
        method="PUT",
        content_length=len(raw),
        content_type=headers["Content-Type"],
        canonical_headers=headers,
        canonical_resource=canonical_resource,
    )
    req_headers = dict(headers)
    req_headers["Authorization"] = auth
    resp = requests.put(url, data=raw, headers=req_headers, timeout=15)
    if resp.status_code not in (201,):
        raise RuntimeError(f"Blob upload failed ({resp.status_code}): {resp.text[:180]}")
    return url

def _download_blob_via_rest(conn_str: str, container_name: str, blob_name: str):
    parts = _parse_conn_str(conn_str)
    account_name = (parts.get("AccountName") or "").strip()
    account_key = (parts.get("AccountKey") or "").strip()
    endpoint_suffix = (parts.get("EndpointSuffix") or "core.windows.net").strip()
    protocol = (parts.get("DefaultEndpointsProtocol") or "https").strip()
    if not account_name or not account_key:
        raise RuntimeError("Blob REST fallback requires AccountName and AccountKey.")

    encoded_blob_name = quote(blob_name, safe="/-_.~")
    url = f"{protocol}://{account_name}.blob.{endpoint_suffix}/{container_name}/{encoded_blob_name}"
    x_ms_date = _rfc1123_now()
    x_ms_version = "2021-12-02"
    headers = {
        "x-ms-date": x_ms_date,
        "x-ms-version": x_ms_version,
        "Content-Type": "",
        "Content-Length": "0",
    }
    canonical_resource = f"/{account_name}/{container_name}/{blob_name}"
    auth = _shared_key_auth_header(
        account_name=account_name,
        account_key_b64=account_key,
        method="GET",
        content_length=0,
        content_type="",
        canonical_headers=headers,
        canonical_resource=canonical_resource,
    )
    req_headers = {
        "x-ms-date": x_ms_date,
        "x-ms-version": x_ms_version,
        "Authorization": auth,
    }
    resp = requests.get(url, headers=req_headers, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Blob download failed ({resp.status_code}): {resp.text[:180]}")
    return resp.content

def _safe_image_filename(name: str, fallback_ext: str = "png") -> str:
    raw = os.path.basename(str(name or "")).strip()
    if "." in raw:
        stem, ext = raw.rsplit(".", 1)
        ext = ext.lower()
    else:
        stem, ext = raw, ""
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-")
    if not stem:
        stem = "image"
    if ext not in CHAT_ALLOWED_IMAGE_EXT:
        ext = fallback_ext
    return f"{stem}.{ext}"

def upload_chat_image_to_blob(upload, session_id):
    if not upload or not upload.filename:
        return None
    mime = (upload.mimetype or "").lower()
    ext = upload.filename.rsplit(".", 1)[-1].lower() if "." in upload.filename else ""
    if ext and ext not in CHAT_ALLOWED_IMAGE_EXT:
        raise ValueError("Unsupported image format.")
    if mime and not mime.startswith("image/"):
        raise ValueError("Attached file must be an image.")

    raw = b""
    try:
        raw = upload.read()
    finally:
        try:
            upload.close()
        except Exception:
            pass

    if not raw:
        raise ValueError("Image file is empty.")
    max_bytes = CHAT_IMAGE_MAX_FILE_MB * 1024 * 1024
    if len(raw) > max_bytes:
        raise ValueError(f"Image is too large. Max size is {CHAT_IMAGE_MAX_FILE_MB} MB.")

    fallback_ext = (mime.split("/", 1)[1] if "/" in mime else "png").lower()
    if fallback_ext == "jpeg":
        fallback_ext = "jpg"
    safe_name = _safe_image_filename(upload.filename, fallback_ext=fallback_ext if fallback_ext in CHAT_ALLOWED_IMAGE_EXT else "png")
    original_filename = os.path.basename(str(upload.filename or "")).strip() or safe_name
    sid = re.sub(r"[^A-Za-z0-9_-]+", "_", str(session_id or "anon"))
    date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    prefix = CHAT_IMAGE_BLOB_PREFIX or "images"
    blob_name = f"{prefix}/{sid}/{date_path}/{uuid.uuid4().hex}_{safe_name}"

    blob_url = ""
    container_name = str(CHAT_IMAGE_CONTAINER or "").strip()
    if _has_blob_storage_config():
        if BLOB_AVAILABLE:
            container = _blob_container(container_name)
            blob_client = container.get_blob_client(blob_name)
            kwargs = {"overwrite": True}
            if ContentSettings:
                kwargs["content_settings"] = ContentSettings(content_type=mime or "application/octet-stream")
            blob_client.upload_blob(raw, **kwargs)
            blob_url = blob_client.url
        else:
            # Fallback for environments missing azure-storage-blob dependency.
            blob_url = _upload_chat_image_via_rest(
                conn_str=_blob_conn_str(),
                container_name=container_name,
                blob_name=blob_name,
                raw=raw,
                mime=mime or "application/octet-stream",
            )
    else:
        _store_chat_image_locally(blob_name, raw)
        container_name = LOCAL_CHAT_IMAGE_CONTAINER

    return {
        "container": container_name,
        "blob_name": blob_name,
        "blob_url": blob_url,
        "filename": original_filename,
        "mime": mime or "",
        "size": len(raw),
    }

def upload_changefeed_image_to_blob(upload):
    # Reuse the existing upload pipeline and storage container/prefix.
    # We separate blob path by a fixed pseudo-session id.
    if not upload or not getattr(upload, "filename", ""):
        return None
    return upload_chat_image_to_blob(upload, "changefeed")

def init_tables():
    svc = _svc()
    for name in [REGISTER_TABLE, CHATS_TABLE, CONFIG_TABLE, CONTACT_TABLE, CHANGEFEED_TABLE]:
        try:
            svc.create_table(table_name=name)
        except ResourceExistsError:
            pass

def now_utc():
    return datetime.now(timezone.utc)

def parse_utc_dt(value):
    if isinstance(value, datetime):
        dt = value
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None

def to_utc_iso_z(value):
    dt = parse_utc_dt(value)
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")

def utc_now_iso():
    return to_utc_iso_z(now_utc())

def stringify_ts(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return to_utc_iso_z(value)
    return str(value)

def json_dt_default(value):
    if isinstance(value, datetime):
        return to_utc_iso_z(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

def _coerce_graph_payload(raw):
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        return {}
    text = raw.strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return dict(data) if isinstance(data, dict) else {}

def _normalize_graph_value(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None

def _normalize_graph_number(value):
    if value is None or value == "":
        return None
    try:
        num = float(value)
    except Exception:
        return None
    if num != num or num in (float("inf"), float("-inf")):
        return None
    return num

def normalize_message_graph(entity) -> dict | None:
    if not isinstance(entity, dict):
        return None
    payload = _coerce_graph_payload(
        entity.get("graph_payload")
        or entity.get("graph_json")
        or entity.get("graph")
    )
    merged = {}
    merged.update(payload)
    merged.update({k: v for k, v in entity.items() if v not in (None, "")})

    graph_type = _normalize_graph_value(
        merged.get("graph_type")
        or merged.get("type")
    )
    expression = _normalize_graph_value(
        merged.get("graph_expression")
        or merged.get("expression")
        or merged.get("function")
    )
    title = _normalize_graph_value(
        merged.get("graph_title")
        or merged.get("title")
    )
    subtitle = _normalize_graph_value(
        merged.get("graph_subtitle")
        or merged.get("subtitle")
    )
    hint = _normalize_graph_value(
        merged.get("graph_hint")
        or merged.get("hint")
    )

    if not graph_type and expression:
        graph_type = "function"
    if graph_type != "function" or not expression:
        return None

    graph = {
        "graph_type": "function",
        "graph_expression": expression,
    }
    if title:
        graph["graph_title"] = title
    if subtitle:
        graph["graph_subtitle"] = subtitle
    if hint:
        graph["graph_hint"] = hint

    for source_key, target_key in [
        ("graph_x_label", "graph_x_label"),
        ("graph_y_label", "graph_y_label"),
        ("x_label", "graph_x_label"),
        ("y_label", "graph_y_label"),
    ]:
        value = _normalize_graph_value(merged.get(source_key))
        if value and target_key not in graph:
            graph[target_key] = value

    for source_key, target_key in [
        ("graph_x_min", "graph_x_min"),
        ("graph_x_max", "graph_x_max"),
        ("graph_y_min", "graph_y_min"),
        ("graph_y_max", "graph_y_max"),
        ("x_min", "graph_x_min"),
        ("x_max", "graph_x_max"),
        ("y_min", "graph_y_min"),
        ("y_max", "graph_y_max"),
    ]:
        value = _normalize_graph_number(merged.get(source_key))
        if value is not None and target_key not in graph:
            graph[target_key] = value

    return graph

GRAPH_REQUEST_RE = re.compile(r"\b(graph|plot|draw|display|visuali[sz]e|curve)\b", re.IGNORECASE)
GRAPH_LATEX_RE = re.compile(r"\$\$([^$]+)\$\$|\$([^$]+)\$")

def message_requests_graph(text: str) -> bool:
    return bool(GRAPH_REQUEST_RE.search(str(text or "")))

def extract_graph_expression_from_text(text: str, fallback_text: str = "") -> str | None:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return None
    expr = re.sub(r"\s+", " ", raw)
    patterns = [
        r"^\s*(?:can|could|would)\s+you\s+",
        r"^\s*(?:please\s+)?(?:show|draw|display|visuali(?:s|z)e|plot|graph)\s+(?:me\s+)?(?:the\s+)?(?:graph|plot|function)?\s*(?:of\s+)?",
        r"^\s*(?:i\s+want\s+)?(?:a\s+)?(?:graph|plot)\s+(?:of\s+)?",
        r"^\s*what\s+does\s+(?:the\s+)?(?:graph|plot)\s+(?:of\s+)?",
    ]
    for pattern in patterns:
        expr = re.sub(pattern, "", expr, flags=re.IGNORECASE)
    expr = re.sub(r"^y\s*=\s*", "", expr, flags=re.IGNORECASE)
    expr = re.sub(r"^f\s*\(\s*x\s*\)\s*=\s*", "", expr, flags=re.IGNORECASE)
    expr = re.sub(r"[?.!,]+$", "", expr).strip()
    if re.search(r"(x|\\theta)", expr, re.IGNORECASE):
        return expr

    fallback = str(fallback_text or "")
    match = GRAPH_LATEX_RE.search(fallback)
    if match:
        candidate = str(match.group(1) or match.group(2) or "").strip()
        if re.search(r"(x|\\theta)", candidate, re.IGNORECASE):
            return candidate
    return None

def build_inferred_graph(text: str, fallback_text: str = "") -> dict | None:
    expr = extract_graph_expression_from_text(text, fallback_text=fallback_text)
    if not expr:
        return None
    return {
        "graph_type": "function",
        "graph_expression": expr,
        "graph_title": f"Graph of {expr}",
        "graph_hint": "Graph preview generated from the request. Drag to pan, wheel or +/- to zoom.",
    }

def clean_ocr_text(text: str) -> str:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in raw.split("\n")]
    compact = []
    blank_open = False
    for ln in lines:
        if ln:
            compact.append(ln)
            blank_open = False
            continue
        if compact and not blank_open:
            compact.append("")
            blank_open = True
    while compact and not compact[-1]:
        compact.pop()
    return "\n".join(compact).strip()

def _configure_tesseract_binary():
    if not PYTESSERACT_AVAILABLE:
        return
    tesseract_cmd = (os.getenv("TESSERACT_CMD") or "").strip()
    if not tesseract_cmd:
        known_paths = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        for p in known_paths:
            if os.path.exists(p):
                tesseract_cmd = p
                break
    if tesseract_cmd:
        try:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        except Exception:
            pass

_TESS_LANG_CACHE = None

def _available_tesseract_languages():
    global _TESS_LANG_CACHE
    if _TESS_LANG_CACHE is not None:
        return _TESS_LANG_CACHE
    langs = set()
    try:
        _configure_tesseract_binary()
        got = pytesseract.get_languages(config="")
        langs = {str(x).strip().lower() for x in (got or []) if str(x).strip()}
    except Exception:
        langs = set()
    _TESS_LANG_CACHE = langs
    return langs

def _pick_formula_ocr_lang() -> str:
    preferred = (OCR_LANG or "eng").strip() or "eng"
    langs = _available_tesseract_languages()
    parts = [p.strip() for p in re.split(r"[+,]", preferred) if p.strip()]
    out = []
    for p in parts:
        if p not in out:
            out.append(p)
    if "equ" in langs and "equ" not in out:
        out.append("equ")
    if not out:
        out = ["eng", "equ"] if "equ" in langs else ["eng"]
    return "+".join(out)

def _trim_to_content(gray_img):
    inv = gray_img.point(lambda px: 255 if px < 245 else 0)
    bbox = inv.getbbox()
    if not bbox:
        return gray_img
    w, h = gray_img.size
    pad = max(8, int(min(w, h) * 0.02))
    l = max(0, bbox[0] - pad)
    t = max(0, bbox[1] - pad)
    r = min(w, bbox[2] + pad)
    b = min(h, bbox[3] + pad)
    if (r - l) < 10 or (b - t) < 10:
        return gray_img
    return gray_img.crop((l, t, r, b))

def _prepare_ocr_variants(raw: bytes):
    if not raw:
        return []
    if not PIL_AVAILABLE:
        raise RuntimeError(
            f"OCR image support is unavailable: {PIL_IMPORT_ERROR or 'Pillow not installed.'}"
        )
    if not PYTESSERACT_AVAILABLE:
        raise RuntimeError(
            f"OCR engine support is unavailable: {PYTESSERACT_IMPORT_ERROR or 'pytesseract not installed.'}"
        )
    _configure_tesseract_binary()
    with Image.open(BytesIO(raw)) as opened:
        src = ImageOps.exif_transpose(opened).convert("L")
        src = _trim_to_content(src)
        w, h = src.size
        max_side = 2800
        if max(w, h) > max_side:
            scale = max_side / float(max(w, h))
            next_size = (max(1, int(w * scale)), max(1, int(h * scale)))
            try:
                resample = Image.Resampling.LANCZOS
            except Exception:
                resample = Image.LANCZOS if hasattr(Image, "LANCZOS") else 1
            src = src.resize(next_size, resample=resample)
        min_h = 360
        if src.height < min_h and src.height > 0:
            scale = min_h / float(src.height)
            up_size = (max(1, int(src.width * scale)), min_h)
            try:
                resample = Image.Resampling.LANCZOS
            except Exception:
                resample = Image.LANCZOS if hasattr(Image, "LANCZOS") else 1
            src = src.resize(up_size, resample=resample)
        base = ImageOps.autocontrast(src).filter(ImageFilter.MedianFilter(size=3))
        sharp = base.filter(ImageFilter.UnsharpMask(radius=1.6, percent=180, threshold=2))
        b150 = sharp.point(lambda px: 255 if px > 150 else 0)
        b170 = sharp.point(lambda px: 255 if px > 170 else 0)
        b190 = sharp.point(lambda px: 255 if px > 190 else 0)
        inv170 = ImageOps.invert(b170.convert("L"))
        return [base, sharp, b150, b170, b190, inv170]

def normalize_math_ocr_expression(expr: str) -> str:
    s = str(expr or "").strip()
    if not s:
        return ""
    replacements = {
        "\u2212": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u00d7": r"\times ",
        "\u00f7": r"\div ",
        "\u00b7": r"\cdot ",
        "\u2219": r"\cdot ",
        "\u2264": r"\leq ",
        "\u2265": r"\geq ",
        "\u2260": r"\neq ",
        "\u2248": r"\approx ",
        "\u221e": r"\infty ",
        "\u03c0": r"\pi ",
        "\u03b8": r"\theta ",
        "\u00b2": "^2",
        "\u00b3": "^3",
        "\u2074": "^4",
        "\u2075": "^5",
        "\u2076": "^6",
        "\u2077": "^7",
        "\u2078": "^8",
        "\u2079": "^9",
        "\u2070": "^0",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    s = s.replace("OQ", "dQ")
    s = s.replace("0Q", "dQ")
    s = s.replace("xX", "x")
    s = s.replace("Xx", "x")
    s = s.replace("XX", "x")
    s = s.replace("@", "d")
    s = s.replace("\u00a9", "Q")
    s = s.replace("\u00a2", "Q")
    s = s.replace("|", "")
    s = re.sub(r"(?<=\d)[oO](?=\d)", "0", s)
    s = s.replace("+-", r"\pm ")
    s = s.replace("-+", r"\pm ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^\$+|\$+$", "", s).strip()
    s = re.sub(r"\s*([=+\-*/^(){}\[\],])\s*", r"\1", s)
    s = re.sub(r"(\\sqrt)\s+([A-Za-z0-9]+)", r"\1{\2}", s)
    s = re.sub(r"(\\[a-zA-Z]+)\s+", r"\1 ", s)
    s = re.sub(r"(?<!\\)([A-Za-z][A-Za-z0-9]*)/([A-Za-z][A-Za-z0-9]*)", r"\\frac{\1}{\2}", s)
    s = s.strip(" .,;")
    return s

def _text_candidate_score(txt: str) -> float:
    s = str(txt or "").strip()
    if not s:
        return -999.0
    alnum = sum(1 for ch in s if ch.isalnum())
    bad = len(re.findall(r"[^A-Za-z0-9\s\.,;:!?\-_=+*/^(){}\[\]\\]", s))
    lines = [ln.strip() for ln in s.split("\n") if ln.strip()]
    line_bonus = len(lines) * 1.2
    long_line_bonus = len([ln for ln in lines if len(ln) >= 28]) * 2.2
    noisy_penalty = len([ln for ln in lines if _looks_like_noise_line(ln)]) * 4.0
    return (alnum * 1.0) + line_bonus + long_line_bonus - (bad * 2.0) - noisy_penalty

def math_line_score(line: str) -> float:
    s = str(line or "").strip()
    if not s:
        return -999.0
    symbols = len(re.findall(r"[=+\-*/^<>\\]", s))
    digits = sum(1 for ch in s if ch.isdigit())
    variables = len(re.findall(r"[xyzXYZ]", s))
    math_tokens = len(
        re.findall(
            r"(sin|cos|tan|cot|sec|csc|log|ln|sqrt|frac|int|sum|lim|theta|pi)",
            s,
            flags=re.IGNORECASE,
        )
    )
    words = len(re.findall(r"[A-Za-z]{4,}", s))
    braces = len(re.findall(r"[(){}\[\]]", s))
    score = (symbols * 3.2) + (digits * 1.3) + (variables * 1.7) + (math_tokens * 4.0) + (braces * 0.8) - (words * 1.8)
    if "=" in s:
        score += 2.0
    return score

def line_is_math_like(line: str) -> bool:
    s = str(line or "").strip()
    if not s:
        return False
    score = math_line_score(s)
    has_operator = bool(re.search(r"[=+\-*/^\\]", s))
    has_mixed = bool(re.search(r"[A-Za-z]", s) and re.search(r"\d", s))
    return score >= 5.0 and (has_operator or has_mixed)

def _looks_like_noise_line(line: str) -> bool:
    s = str(line or "").strip()
    if not s:
        return False
    if re.fullmatch(r"\([a-zA-Z]\)", s):
        return False
    plain = re.sub(r"[^A-Za-z0-9]", "", s)
    if not plain:
        return True
    if re.search(r"[~`]", s):
        return True
    if len(s) <= 3 and not re.search(r"\d", s):
        return True
    if re.fullmatch(r"[0-9]{1,3}\s*[A-Za-z]{1,2}", s):
        return True
    tokens = re.findall(r"[A-Za-z0-9]+", s)
    if tokens:
        short_tokens = [t for t in tokens if len(t) <= 2]
        if len(tokens) <= 2 and len(short_tokens) == len(tokens):
            if not any(sym in s for sym in ["=", "+", "-", "*", "/", "^", "\\"]):
                return True
    letters = re.findall(r"[A-Za-z]", s)
    digits = re.findall(r"\d", s)
    if letters and digits and len(s) <= 8 and not any(sym in s for sym in ["=", "+", "-", "*", "/", "^", "\\"]):
        return True
    return False

def _is_confident_math_line(line: str) -> bool:
    norm = normalize_math_ocr_expression(line)
    if not norm:
        return False
    score = math_line_score(norm)
    if r"\frac{" in norm and score >= 3.8:
        return True
    if "=" in norm and score >= 4.3 and len(norm) >= 5:
        return True
    if re.search(r"[+\-*/^]", norm) and re.search(r"\d", norm) and score >= 6.6:
        return True
    return False

def _sanitize_plain_ocr_line(line: str) -> str:
    s = str(line or "").strip()
    if not s:
        return ""
    # Drop common OCR artifacts while preserving readable sentence text.
    s = s.replace("~", " ")
    s = s.replace("¦", " ")
    s = s.replace("©", " ")
    s = s.replace("¢", " ")
    s = re.sub(r"\(([a-zA-Z])\)\s*[}\]]", r"(\1)", s)
    if not _is_confident_math_line(s):
        s = s.replace("{", "").replace("}", "")
        s = s.replace("[", "").replace("]", "")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)
    s = re.sub(r"[`~]+$", "", s).strip()
    return s

def _formula_candidate_score(expr: str) -> float:
    s = normalize_math_ocr_expression(expr)
    if not s:
        return -999.0
    base = math_line_score(s)
    bad = len(re.findall(r"[^A-Za-z0-9=+\-*/^(){}\[\]\\\.,]", s))
    unknown_words = [
        w for w in re.findall(r"[A-Za-z]{4,}", s)
        if not re.fullmatch(
            r"(frac|sqrt|times|div|cdot|theta|pi|sin|cos|tan|cot|sec|csc|log|ln|lim|sum|int|alpha|beta|gamma|delta|lambda|sigma|omega|left|right|pm|leq|geq|neq|approx|infty)",
            w,
            flags=re.IGNORECASE,
        )
    ]
    repeated_noise = len(re.findall(r"([A-Za-z\-])\1{2,}", s))
    if "=" in s:
        base += 2.4
    if r"\frac{" in s:
        base += 3.0
    if r"\sqrt{" in s:
        base += 2.2
    if r"\pm" in s:
        base += 1.6
    if len(s) < 4:
        base -= 8.0
    if re.fullmatch(r"[A-Za-z]=\d", s):
        base -= 8.0
    base -= len(unknown_words) * 8.0
    base -= repeated_noise * 2.6
    base -= bad * 2.0
    return base

def _formula_signature(expr: str) -> str:
    s = str(expr or "").lower()
    if not s:
        return ""
    s = s.replace("√", "v")
    s = s.replace("\\sqrt", "v")
    s = s.replace("×", "x")
    s = s.replace("—", "-").replace("–", "-")
    s = s.replace("±", "+-")
    s = s.replace(" ", "")
    s = s.replace("|", "")
    return s

def _infer_quadratic_formula_candidate(candidates):
    if not candidates:
        return ""
    sig = _formula_signature(" ".join(candidates))
    if not sig:
        return ""
    has_x = ("x=" in sig) or ("=x" in sig) or ("x" in sig)
    has_b = "b" in sig
    has_disc = ("4ac" in sig) or ("4a" in sig and "c" in sig)
    has_two_a = ("2a" in sig) or ("a2" in sig)
    has_root = ("v" in sig) or ("sqrt" in sig)
    has_plusminus = ("+-" in sig) or ("-+" in sig) or ("pm" in sig)
    has_signed_b = ("-b" in sig) or ("+b" in sig) or ("--b" in sig)
    if has_x and has_b and has_disc and has_two_a and has_root and (has_plusminus or has_signed_b):
        return r"x=\frac{-b\pm\sqrt{b^2-4ac}}{2a}"
    return ""

def _assemble_fraction_candidate(lines):
    if not lines:
        return ""
    clean = [normalize_math_ocr_expression(ln) for ln in lines if normalize_math_ocr_expression(ln)]
    if not clean:
        return ""
    for i, ln in enumerate(clean):
        if "=" not in ln:
            continue
        left, right = ln.split("=", 1)
        left = left.strip()
        right = right.strip()
        if i >= 1:
            num = clean[i - 1].strip()
            if num and left and not re.search(r"[=+\-*^]", num) and not re.search(r"[=+\-*^]", left):
                return f"\\frac{{{num}}}{{{left}}}={right}"
        if i >= 2 and not left:
            num = clean[i - 2].strip()
            den = clean[i - 1].strip()
            if num and den:
                return f"\\frac{{{num}}}{{{den}}}={right}"
    if len(clean) >= 2:
        a = clean[0]
        b = clean[1]
        if a and b and not re.search(r"[=+\-*^]", a) and not re.search(r"[=+\-*^]", b):
            return f"\\frac{{{a}}}{{{b}}}"
    return ""

def _pick_primary_rhs_symbol(rhs: str) -> str:
    r = normalize_math_ocr_expression(rhs)
    caps = re.findall(r"[A-Z]", r)
    if caps:
        return caps[-1]
    letters = [ch for ch in re.findall(r"[a-z]", r) if ch not in ("k", "c", "e")]
    if letters:
        return letters[-1]
    return ""

def _infer_derivative_candidate(candidates):
    if not candidates:
        return ""
    normalized = [normalize_math_ocr_expression(c) for c in candidates if normalize_math_ocr_expression(c)]
    if not normalized:
        return ""
    # Prefer explicit derivative denominator hints like dt/dx/dy/dz if present.
    explicit_den = ""
    for c in normalized:
        if "=" in c:
            continue
        m = re.search(r"\bd([txyz])\b", c, flags=re.IGNORECASE)
        if m:
            explicit_den = m.group(1)
            break
    den_var = explicit_den or ""
    if not den_var:
        for c in normalized:
            if "=" in c:
                continue
            m = re.search(r"\bd([a-zA-Z])\b", c)
            if m:
                den_var = m.group(1)
                break
    if not den_var:
        for c in normalized:
            left = c.split("=", 1)[0] if "=" in c else c
            m = re.search(r"\bd([a-zA-Z])\b", left)
            if m:
                den_var = m.group(1)
                break
    rhs = ""
    eq_lines = [c for c in normalized if "=" in c]
    if eq_lines:
        rhs = max(eq_lines, key=_formula_candidate_score).split("=", 1)[1]
    if not rhs:
        rhs = max(normalized, key=_formula_candidate_score)
    var = _pick_primary_rhs_symbol(rhs)
    if den_var and var and den_var.lower() == var.lower():
        for c in normalized:
            if "=" in c:
                continue
            m = re.search(r"\bd([a-zA-Z])\b", c)
            if m and m.group(1).lower() != var.lower():
                den_var = m.group(1)
                break
    if not den_var or not var:
        return ""
    right = normalize_math_ocr_expression(rhs).strip()
    if not right:
        return ""
    return f"\\frac{{d{var}}}{{d{den_var}}}={right}"

def format_ocr_text_for_chat(text: str) -> str:
    raw = clean_ocr_text(text)
    lines = raw.split("\n")
    rendered = []
    dropped = 0
    for line in lines:
        ln = line.strip()
        if not ln:
            rendered.append("")
            continue
        cleaned = _sanitize_plain_ocr_line(ln)
        if not cleaned:
            dropped += 1
            continue
        if _looks_like_noise_line(cleaned) and not _is_confident_math_line(cleaned):
            dropped += 1
            continue
        if _is_confident_math_line(cleaned):
            rendered.append(f"$${normalize_math_ocr_expression(cleaned)}$$")
        else:
            rendered.append(cleaned)
    out = clean_ocr_text("\n".join(rendered))
    if not out:
        return raw
    # If cleanup removed too much, keep safer original text.
    kept = len([ln for ln in out.split("\n") if ln.strip()])
    original = len([ln for ln in lines if ln.strip()])
    if original > 0 and kept <= max(1, original // 3):
        return raw
    return out

def extract_text_from_image_bytes(raw: bytes) -> str:
    variants = _prepare_ocr_variants(raw)
    if not variants:
        return ""
    try:
        candidates = []
        whitelist = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-=/*^().,[]{}\\\\"
        for img in variants:
            for config in [
                "--oem 3 --psm 6",
                "--oem 3 --psm 11",
                "--oem 3 --psm 4",
                f"--oem 3 --psm 6 -c tessedit_char_whitelist={whitelist}",
            ]:
                out = pytesseract.image_to_string(img, lang=OCR_LANG, config=config)
                txt = clean_ocr_text(out)
                if txt:
                    candidates.append(txt)
    except Exception as exc:
        if exc.__class__.__name__ == "TesseractNotFoundError":
            raise RuntimeError(
                "Tesseract OCR binary is not installed on the server. "
                "Install Tesseract or set TESSERACT_CMD."
            )
        raise RuntimeError(f"Failed to process image for OCR: {str(exc)}")

    if not candidates:
        return ""
    return max(candidates, key=_text_candidate_score).strip()

def extract_formula_latex_from_image_bytes(raw: bytes) -> str:
    variants = _prepare_ocr_variants(raw)
    if not variants:
        return ""
    try:
        lang = _pick_formula_ocr_lang()
        whitelist = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ+-=/*^().,[]{}\\\\"
        candidates = []
        for img in variants:
            for config in [
                "--oem 1 --psm 7 -c preserve_interword_spaces=1",
                "--oem 1 --psm 6 -c preserve_interword_spaces=1",
                "--oem 1 --psm 11 -c preserve_interword_spaces=1",
                "--oem 1 --psm 13 -c preserve_interword_spaces=1",
                "--oem 1 --psm 3 -c preserve_interword_spaces=1",
                "--oem 1 --psm 4 -c preserve_interword_spaces=1",
                f"--oem 1 --psm 7 -c preserve_interword_spaces=1 -c tessedit_char_whitelist={whitelist}",
                f"--oem 1 --psm 6 -c preserve_interword_spaces=1 -c tessedit_char_whitelist={whitelist}",
                f"--oem 1 --psm 11 -c preserve_interword_spaces=1 -c tessedit_char_whitelist={whitelist}",
            ]:
                out = clean_ocr_text(pytesseract.image_to_string(img, lang=lang, config=config))
                if not out:
                    continue
                lines = [ln.strip() for ln in out.split("\n") if ln.strip()]
                for line in lines:
                    candidates.append(line)
                if lines:
                    joined = normalize_math_ocr_expression(" ".join(lines))
                    if joined:
                        candidates.append(joined)
                    frac = _assemble_fraction_candidate(lines)
                    if frac:
                        candidates.append(frac)
        inferred = _infer_derivative_candidate(candidates)
        if inferred:
            candidates.append(inferred)
        inferred_quad = _infer_quadratic_formula_candidate(candidates)
        if inferred_quad:
            candidates.append(inferred_quad)
    except Exception as exc:
        if exc.__class__.__name__ == "TesseractNotFoundError":
            raise RuntimeError(
                "Tesseract OCR binary is not installed on the server. "
                "Install Tesseract or set TESSERACT_CMD."
            )
        raise RuntimeError(f"Failed to process image for OCR: {str(exc)}")

    if not candidates:
        return ""
    best_line = max(candidates, key=_formula_candidate_score)
    if _formula_candidate_score(best_line) < 4.5:
        return ""
    return normalize_math_ocr_expression(best_line)

def hash_pw(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def is_strong_password(password: str) -> bool:
    if not password or len(password) < 8:
        return False
    return (
        any(c.isupper() for c in password)
        and any(c.islower() for c in password)
        and any(c.isdigit() for c in password)
        and any(c in '@$!%*?&' for c in password)
    )

def new_session_id() -> str:
    return "".join(str(random.randint(0,9)) for _ in range(9))

# ------------- Config helpers (announcements/maintenance) -------------
CONFIG_CACHE = {}

def _config_key(key: str) -> str:
    return f"CONFIG::{key}"

def get_config(key: str):
    try:
        tbl = _table(CONFIG_TABLE)
        return tbl.get_entity(partition_key="CONFIG", row_key=key)
    except ResourceNotFoundError:
        return None
    except Exception:
        return None

def set_config(key: str, data: dict):
    tbl = _table(CONFIG_TABLE)
    ent = {"PartitionKey": "CONFIG", "RowKey": key, **(data or {}), "updated_at": utc_now_iso()}
    tbl.upsert_entity(ent)
    CONFIG_CACHE[_config_key(key)] = {"ts": now_utc(), "val": ent}
    return ent

def delete_config(key: str):
    tbl = _table(CONFIG_TABLE)
    try:
        tbl.delete_entity(partition_key="CONFIG", row_key=key)
    except ResourceNotFoundError:
        pass
    CONFIG_CACHE.pop(_config_key(key), None)

def get_config_cached(key: str, ttl_seconds: int = 20):
    k = _config_key(key)
    item = CONFIG_CACHE.get(k)
    if item:
        dt = item.get("ts")
        try:
            age = (now_utc() - dt).total_seconds()
            if age <= ttl_seconds:
                return item.get("val")
        except Exception:
            pass
    val = get_config(key)
    CONFIG_CACHE[k] = {"ts": now_utc(), "val": val}
    return val

def normalize_timezone_name(value):
    tz = (value or "").strip()
    if not tz:
        return None
    # Accept common IANA names even when tz database is unavailable.
    if "/" not in tz and tz != "UTC":
        return None
    if ZoneInfo is None:
        return tz
    try:
        ZoneInfo(tz)
        return tz
    except Exception:
        return tz

def get_admin_preferences():
    try:
        ent = get_config_cached("admin_preferences") or {}
        tz = normalize_timezone_name(ent.get("timezone")) or "UTC"
        return {"timezone": tz}
    except Exception:
        return {"timezone": "UTC"}

def set_admin_timezone_preference(timezone_name: str):
    tz = normalize_timezone_name(timezone_name)
    if not tz:
        raise ValueError("Invalid timezone name.")
    set_config("admin_preferences", {"timezone": tz})
    return tz

def _to_bool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")

def _to_int(value, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default

def parse_local_datetime_to_utc_iso(local_value: str, tz_offset_minutes=0, tz_name=None):
    s = (local_value or "").strip()
    if not s:
        return None
    dt_local = None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt_local = datetime.strptime(s, fmt)
            break
        except Exception:
            continue
    if not dt_local:
        return None
    tz_norm = normalize_timezone_name(tz_name)
    if tz_norm and ZoneInfo is not None:
        try:
            dt_local_tz = dt_local.replace(tzinfo=ZoneInfo(tz_norm))
            return to_utc_iso_z(dt_local_tz.astimezone(timezone.utc))
        except Exception:
            pass
    # JS getTimezoneOffset(): UTC = local + offset_minutes
    offset = _to_int(tz_offset_minutes, 0)
    dt_utc = (dt_local + timedelta(minutes=offset)).replace(tzinfo=timezone.utc)
    return to_utc_iso_z(dt_utc)

def parse_weekdays_field(value):
    if value is None:
        return []
    items = value if isinstance(value, list) else [value]
    out = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str) and "," in item:
            parts = item.split(",")
        else:
            parts = [item]
        for part in parts:
            try:
                n = int(str(part).strip())
                if 0 <= n <= 6 and n not in out:
                    out.append(n)
            except Exception:
                continue
    out.sort()
    return out

def parse_hhmm(value):
    s = (value or "").strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) < 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except Exception:
        return None
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return hh * 60 + mm

def resolve_schedule_local_now(now_dt, tz_name=None, tz_offset_minutes=0):
    tz_norm = normalize_timezone_name(tz_name)
    if tz_norm and ZoneInfo is not None:
        try:
            return now_dt.astimezone(ZoneInfo(tz_norm))
        except Exception:
            pass
    offset = _to_int(tz_offset_minutes, 0)
    # JS getTimezoneOffset(): UTC = local + offset => local = UTC - offset
    return now_dt - timedelta(minutes=offset)

def schedule_weekday_is_active(weekdays=None, tz_offset_minutes=0, tz_name=None, now=None) -> bool:
    days = parse_weekdays_field(weekdays)
    if not days:
        return True
    now_dt = now or now_utc()
    local_now = resolve_schedule_local_now(now_dt, tz_name=tz_name, tz_offset_minutes=tz_offset_minutes)
    return local_now.weekday() in days

def schedule_time_of_day_is_active(start_time=None, end_time=None, tz_offset_minutes=0, tz_name=None, now=None) -> bool:
    start_m = parse_hhmm(start_time)
    end_m = parse_hhmm(end_time)
    if start_m is None and end_m is None:
        return True
    now_dt = now or now_utc()
    local_now = resolve_schedule_local_now(now_dt, tz_name=tz_name, tz_offset_minutes=tz_offset_minutes)
    now_m = local_now.hour * 60 + local_now.minute
    if start_m is not None and end_m is not None:
        if start_m <= end_m:
            return start_m <= now_m < end_m
        # Overnight window (e.g. 22:00 -> 02:00)
        return now_m >= start_m or now_m < end_m
    if start_m is not None:
        return now_m >= start_m
    return now_m < end_m

def schedule_window_is_active(start_at=None, end_at=None, now=None, weekdays=None, tz_offset_minutes=0, tz_name=None, recurring_start_time=None, recurring_end_time=None) -> bool:
    now_dt = now or now_utc()
    start_dt = parse_utc_dt(start_at)
    end_dt = parse_utc_dt(end_at)
    if start_dt and now_dt < start_dt:
        return False
    if end_dt and now_dt > end_dt:
        return False
    if weekdays and not schedule_weekday_is_active(weekdays, tz_offset_minutes=tz_offset_minutes, tz_name=tz_name, now=now_dt):
        return False
    if (recurring_start_time or recurring_end_time) and not schedule_time_of_day_is_active(
        recurring_start_time,
        recurring_end_time,
        tz_offset_minutes=tz_offset_minutes,
        tz_name=tz_name,
        now=now_dt,
    ):
        return False
    return True

def get_announcement():
    try:
        # Prefer in-memory state set by admin save
        ent = ANNOUNCEMENT_STATE or get_config_cached("announcement") or {}
        if not ent:
            return None
        active = bool(ent.get("active"))
        scheduled_enabled = _to_bool(ent.get("scheduled_enabled"))
        recurring_enabled = _to_bool(ent.get("recurring_enabled"))
        weekdays = parse_weekdays_field(ent.get("weekdays"))
        tz_offset_minutes = _to_int(ent.get("tz_offset_minutes"), 0)
        tz_name = normalize_timezone_name(ent.get("tz_name"))
        recurring_start_time = (ent.get("recurring_start_time") or "").strip()
        recurring_end_time = (ent.get("recurring_end_time") or "").strip()
        start_at = to_utc_iso_z(ent.get("start_at")) if ent.get("start_at") else None
        end_at = to_utc_iso_z(ent.get("end_at")) if ent.get("end_at") else None
        if active and (scheduled_enabled or recurring_enabled):
            if not schedule_window_is_active(
                start_at,
                end_at,
                weekdays=weekdays if recurring_enabled else None,
                tz_offset_minutes=tz_offset_minutes,
                tz_name=tz_name,
                recurring_start_time=recurring_start_time if recurring_enabled else None,
                recurring_end_time=recurring_end_time if recurring_enabled else None,
            ):
                return None
        if active:
            return {
                "title": ent.get("title", "Announcement"),
                "message": ent.get("message", ""),
                "level": ent.get("level", "info"),
                "updated_at": ent.get("updated_at"),
                "scheduled_enabled": scheduled_enabled,
                "recurring_enabled": recurring_enabled,
                "weekdays": weekdays,
                "tz_offset_minutes": tz_offset_minutes,
                "tz_name": tz_name,
                "recurring_start_time": recurring_start_time,
                "recurring_end_time": recurring_end_time,
                "start_at": start_at,
                "end_at": end_at,
            }
    except Exception:
        pass
    return None

def get_maintenance():
    try:
        # Prefer in-memory state set by admin save
        ent = MAINTENANCE_STATE or get_config_cached("maintenance") or {}
        active = bool(ent.get("active", False))
        scheduled_enabled = _to_bool(ent.get("scheduled_enabled"))
        recurring_enabled = _to_bool(ent.get("recurring_enabled"))
        weekdays = parse_weekdays_field(ent.get("weekdays"))
        tz_offset_minutes = _to_int(ent.get("tz_offset_minutes"), 0)
        tz_name = normalize_timezone_name(ent.get("tz_name"))
        recurring_start_time = (ent.get("recurring_start_time") or "").strip()
        recurring_end_time = (ent.get("recurring_end_time") or "").strip()
        start_at = to_utc_iso_z(ent.get("start_at")) if ent.get("start_at") else None
        end_at = to_utc_iso_z(ent.get("end_at")) if ent.get("end_at") else None
        if active and (scheduled_enabled or recurring_enabled):
            active = schedule_window_is_active(
                start_at,
                end_at,
                weekdays=weekdays if recurring_enabled else None,
                tz_offset_minutes=tz_offset_minutes,
                tz_name=tz_name,
                recurring_start_time=recurring_start_time if recurring_enabled else None,
                recurring_end_time=recurring_end_time if recurring_enabled else None,
            )
        return {
            "active": active,
            "message": ent.get("message", "The system is under maintenance."),
            "scheduled_enabled": scheduled_enabled,
            "recurring_enabled": recurring_enabled,
            "weekdays": weekdays,
            "tz_offset_minutes": tz_offset_minutes,
            "tz_name": tz_name,
            "recurring_start_time": recurring_start_time,
            "recurring_end_time": recurring_end_time,
            "start_at": start_at,
            "end_at": end_at,
        }
    except Exception:
        return {"active": False, "message": "The system is under maintenance."}

# ------------- Webhook helpers -------------
def get_webhook_env():
    global WEBHOOK_ENV_STATE
    if WEBHOOK_ENV_STATE:
        return WEBHOOK_ENV_STATE
    ent = get_config_cached("webhook") or {}
    env = (ent.get("env") if isinstance(ent, dict) else getattr(ent, "env", None)) or "test"
    return env if env in ("test", "prod") else "test"

def set_webhook_env(env: str):
    global WEBHOOK_ENV_STATE
    env = env if env in ("test", "prod") else "test"
    WEBHOOK_ENV_STATE = env
    set_config("webhook", {"env": env})
    return env

def get_webhook_url():
    env = get_webhook_env()
    return N8N_PROD_WEBHOOK if env == "prod" else N8N_TEST_WEBHOOK


# ------------- Template globals -------------
@app.context_processor
def inject_globals():
    try:
        env = get_webhook_env()
    except Exception:
        env = "test"
    try:
        admin_pref_tz = get_admin_preferences().get("timezone", "UTC")
    except Exception:
        admin_pref_tz = "UTC"
    username = session.get("username")
    is_admin = bool(session.get("is_admin"))
    return {
        "app_version": APP_VERSION,
        "webhook_env": env,
        "current_username": username,
        "current_session_id": session.get("session_id"),
        "is_logged_in": bool(username),
        "is_admin_user": is_admin,
        "admin_timezone": admin_pref_tz if is_admin else None,
    }

def get_user(username: str):
    tbl = _table(REGISTER_TABLE)
    try:
        return tbl.get_entity(partition_key="USER", row_key=username)
    except ResourceNotFoundError:
        return None

def create_user(username: str, password: str):
    tbl = _table(REGISTER_TABLE)
    if get_user(username):
        raise ValueError("Username already exists")
    sess_id = new_session_id()
    ent = {
        "PartitionKey": "USER",
        "RowKey": username,
        "password_hash": hash_pw(password),
        "session_id": sess_id,
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }
    tbl.create_entity(entity=ent)
    return ent

def authenticate(username: str, password: str):
    u = get_user(username)
    if not u:
        return None
    # Deny login if account is locked
    if u.get("locked"):
        return None
    if u.get("password_hash") == hash_pw(password):
        return u
    return None

# ------------- Change feed (admin-authored articles) -------------
def _changefeed_table():
    return _table(CHANGEFEED_TABLE)

def list_changefeed_items(limit: int = 100):
    tbl = _changefeed_table()
    try:
        items = list(
            tbl.query_entities(
                "PartitionKey eq 'FEED'",
                select=[
                    "PartitionKey", "RowKey", "created_at", "updated_at",
                    "title", "body",
                    "image_url", "image_filename", "image_container", "image_blob_name", "image_mime", "image_size",
                ],
            )
        )
    except Exception:
        items = []
    def _dt(ent):
        return parse_utc_dt(ent.get("created_at")) or parse_utc_dt(ent.get("Timestamp")) or datetime.min.replace(tzinfo=timezone.utc)
    items.sort(key=_dt, reverse=True)
    out = []
    for ent in items[: max(1, int(limit or 100))]:
        rk = ent.get("RowKey")
        image_blob_name = str(ent.get("image_blob_name") or "").strip()
        image_preview_url = str(ent.get("image_url") or "").strip()
        if rk and image_blob_name:
            try:
                image_preview_url = url_for("changefeed_image_proxy", row_key=rk)
            except Exception:
                pass
        out.append(
            {
                "RowKey": rk,
                "title": ent.get("title") or "",
                "body": ent.get("body") or "",
                "created_at": to_utc_iso_z(ent.get("created_at")) or to_utc_iso_z(ent.get("Timestamp")) or utc_now_iso(),
                "updated_at": to_utc_iso_z(ent.get("updated_at")) or "",
                "image_url": str(ent.get("image_url") or "").strip(),
                "image_preview_url": image_preview_url,
                "image_filename": str(ent.get("image_filename") or "").strip(),
                "image_container": str(ent.get("image_container") or "").strip(),
                "image_blob_name": image_blob_name,
                "image_mime": str(ent.get("image_mime") or "").strip(),
                "image_size": ent.get("image_size"),
            }
        )
    return out

def get_changefeed_item(row_key: str):
    if not row_key:
        return None
    try:
        return _changefeed_table().get_entity(partition_key="FEED", row_key=row_key)
    except Exception:
        return None

def create_changefeed_item(title: str, body: str, image_info=None):
    title = str(title or "").strip()
    body = str(body or "").strip()
    if not title:
        raise ValueError("Title is required.")
    if not body:
        raise ValueError("Body is required.")
    row_key = f"{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex}"
    ent = {
        "PartitionKey": "FEED",
        "RowKey": row_key,
        "title": title,
        "body": body,
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }
    if isinstance(image_info, dict):
        image_url = str(image_info.get("blob_url") or "").strip()
        image_filename = str(image_info.get("filename") or "").strip()
        image_container = str(image_info.get("container") or "").strip()
        image_blob_name = str(image_info.get("blob_name") or "").strip()
        image_mime = str(image_info.get("mime") or "").strip()
        image_size = image_info.get("size")
        if image_url:
            ent["image_url"] = image_url
        if image_filename:
            ent["image_filename"] = image_filename
        if image_container:
            ent["image_container"] = image_container
        if image_blob_name:
            ent["image_blob_name"] = image_blob_name
        if image_mime:
            ent["image_mime"] = image_mime
        try:
            if image_size is not None:
                ent["image_size"] = int(image_size)
        except Exception:
            pass
    _changefeed_table().create_entity(ent)
    return ent

def delete_changefeed_item(row_key: str):
    ent = get_changefeed_item(row_key)
    if not ent:
        return False
    container_name = str(ent.get("image_container") or CHAT_IMAGE_CONTAINER).strip()
    blob_name = str(ent.get("image_blob_name") or "").strip()
    # Delete entity first (so UI updates even if blob cleanup fails)
    _changefeed_table().delete_entity(partition_key="FEED", row_key=row_key)
    if blob_name:
        try:
            if container_name == LOCAL_CHAT_IMAGE_CONTAINER:
                _delete_chat_image_locally(blob_name)
            elif BLOB_AVAILABLE:
                _blob_container(container_name).get_blob_client(blob_name).delete_blob()
        except Exception:
            pass
    return True


def save_contact_request(kind: str, name: str, email: str, message: str, extra=None):
    tbl = _table(CONTACT_TABLE)
    entity = {
        "PartitionKey": str(kind or "general").strip() or "general",
        "RowKey": str(uuid.uuid4()),
        "name": str(name or "").strip(),
        "email": str(email or "").strip(),
        "message": str(message or "").strip(),
        "created_at": utc_now_iso(),
    }
    if isinstance(extra, dict):
        for key, value in extra.items():
            if value is None:
                continue
            entity[str(key)] = str(value).strip() if isinstance(value, str) else value
    tbl.create_entity(entity=entity)
    return entity


def submit_formspree_request(endpoint: str, payload: dict):
    url = str(endpoint or "").strip()
    if not url:
        return None
    resp = requests.post(
        url,
        data=payload,
        headers={"Accept": "application/json"},
        timeout=15,
    )
    if resp.status_code >= 400:
        detail = ""
        try:
            data = resp.json()
            if isinstance(data, dict):
                if data.get("errors"):
                    detail = "; ".join(
                        str(item.get("message") or item)
                        for item in data.get("errors", [])
                    )
                else:
                    detail = str(data.get("error") or data.get("message") or "").strip()
        except Exception:
            detail = (resp.text or "").strip()
        raise RuntimeError(detail or f"Form submission failed with status {resp.status_code}.")
    return resp

def post_message(session_id: str, sender: str, text: str, attachment=None):
    tbl = _table(CHATS_TABLE)
    entity = {
        "PartitionKey": session_id,
        "RowKey": str(uuid.uuid4()),
        "sender": sender,
        "text": text,
        "created_at": utc_now_iso(),
    }
    if isinstance(attachment, dict):
        image_url = str(attachment.get("image_url") or "").strip()
        image_filename = str(attachment.get("image_filename") or "").strip()
        image_container = str(attachment.get("image_container") or "").strip()
        image_blob_name = str(attachment.get("image_blob_name") or "").strip()
        image_mime = str(attachment.get("image_mime") or "").strip()
        image_size = attachment.get("image_size")
        if image_url:
            entity["image_url"] = image_url
        if image_filename:
            entity["image_filename"] = image_filename
        if image_container:
            entity["image_container"] = image_container
        if image_blob_name:
            entity["image_blob_name"] = image_blob_name
        if image_mime:
            entity["image_mime"] = image_mime
        try:
            if image_size is not None:
                entity["image_size"] = int(image_size)
        except Exception:
            pass
    tbl.create_entity(entity=entity)
    return entity

def list_messages_v2(session_id: str, since_iso=None, limit: int = 200):
    """
    Ordered timeline using Azure Table's Timestamp (server clock) when available.
    Normalizes created_at to UTC ISO; merges user rows and assistant rows addressed via sendto.
    """
    tbl = _table(CHATS_TABLE)

    def get_dt(ent):
        return parse_utc_dt(ent.get("Timestamp")) or parse_utc_dt(ent.get("created_at")) or now_utc()

    entries = []

    def push(ent, sender):
        dt = get_dt(ent)
        rowkey = ent.get("RowKey")
        image_url = ent.get("image_url", "")
        image_blob_name = ent.get("image_blob_name", "")
        image_preview_url = image_url
        if rowkey and image_blob_name:
            try:
                image_preview_url = url_for("chat_image_proxy", row_key=rowkey)
            except Exception:
                image_preview_url = image_url
        obj = {
            "PartitionKey": ent.get("PartitionKey"),
            "RowKey": rowkey,
            "sender": sender,
            "text": ent.get("text", ""),
            "image_url": image_url,
            "image_preview_url": image_preview_url,
            "image_filename": ent.get("image_filename", ""),
            "image_container": ent.get("image_container", ""),
            "image_blob_name": image_blob_name,
            "image_mime": ent.get("image_mime", ""),
            "image_size": ent.get("image_size"),
            "created_at": to_utc_iso_z(dt),
        }
        graph = normalize_message_graph(ent)
        if graph:
            obj["graph"] = graph
        entries.append((dt, 1 if sender == "assistant" else 0, rowkey or "", obj))

    query_fields = [
        "PartitionKey", "RowKey", "sender", "text", "sendto", "Timestamp", "created_at",
        "image_url", "image_filename", "image_container", "image_blob_name", "image_mime", "image_size",
        "graph_type", "graph_expression", "graph_title", "graph_subtitle", "graph_hint",
        "graph_x_label", "graph_y_label", "graph_x_min", "graph_x_max", "graph_y_min", "graph_y_max",
        "graph_payload", "graph_json",
    ]

    # User rows (PartitionKey == session_id)
    try:
        user_rows = list(tbl.query_entities(f"PartitionKey eq '{session_id}'", select=query_fields))
    except Exception:
        user_rows = []
    for m in user_rows:
        push(m, m.get("sender", "user"))

    # Assistant rows addressed to this session via sendto or AI partition
    try:
        sendto_rows = list(
            tbl.query_entities(
                f"sendto eq '{session_id}' or sendto eq 'session_{session_id}'",
                select=query_fields,
            )
        )
    except Exception:
        sendto_rows = []
    try:
        ai_rows = list(tbl.query_entities(f"PartitionKey eq 'AI_{session_id}'", select=query_fields))
    except Exception:
        ai_rows = []

    seen_rowkeys = set()
    for m in (sendto_rows + ai_rows):
        rk = m.get("RowKey")
        if rk and rk in seen_rowkeys:
            continue
        if rk:
            seen_rowkeys.add(rk)
        push(m, "assistant")

    # Strict chronological order; for same timestamp keep user before assistant, then RowKey
    entries.sort(key=lambda t: (t[0], t[1], t[2]))

    pending_graph = None
    for _, _, _, msg in entries:
        sender = str(msg.get("sender") or "")
        if sender == "user":
            if message_requests_graph(msg.get("text", "")):
                pending_graph = build_inferred_graph(msg.get("text", ""))
            else:
                pending_graph = None
            continue
        if sender == "assistant":
            if not msg.get("graph") and pending_graph:
                msg["graph"] = dict(pending_graph)
            pending_graph = None

    if since_iso:
        sdt = parse_utc_dt(since_iso)
        if sdt:
            entries = [e for e in entries if e[0] > sdt]

    return [e[3] for e in entries][-limit:]

def list_messages(session_id: str, since_iso=None, limit: int = 200):
    """
    Collect messages for a session from two sources:
    - User messages we store with PartitionKey == session_id
    - Assistant messages written by external flow into chat table where sendto == f"session_{session_id}" (or == session_id)

    Normalize each message to have keys: sender in {"user","assistant"}, text, created_at (ISO8601).
    """
    tbl = _table(CHATS_TABLE)

    def to_iso(dt):
        try:
            if isinstance(dt, datetime):
                return to_utc_iso_z(dt)
        except Exception:
            pass
        return None

    # Pull everything then filter; acceptable for prototype scale
    raw = list(tbl.list_entities())

    out = []
    # User-authored messages for this session
    for m in raw:
        if m.get("PartitionKey") == session_id:
            created = to_utc_iso_z(m.get("created_at")) or to_iso(m.get("Timestamp")) or utc_now_iso()
            out.append({
                "PartitionKey": session_id,
                "RowKey": m.get("RowKey"),
                "sender": m.get("sender", "user"),
                "text": m.get("text", ""),
                "created_at": created,
            })

    # Assistant replies addressed to this session via sendto
    targets = {session_id, f"session_{session_id}"}
    for m in raw:
        sendto = m.get("sendto")
        if sendto and sendto in targets:
            created = to_utc_iso_z(m.get("created_at")) or to_iso(m.get("Timestamp")) or utc_now_iso()
            out.append({
                "PartitionKey": m.get("PartitionKey"),
                "RowKey": m.get("RowKey"),
                "sender": "assistant",  # normalize external 'A' to 'assistant'
                "text": m.get("text", ""),
                "created_at": created,
            })

    # Order and apply since/limit
    def parse_dt(s):
        return parse_utc_dt(s) or datetime.min.replace(tzinfo=timezone.utc)

    # Stable sort: time → role weight (user first if tie) → RowKey
    def role_weight(sender: str) -> int:
        return 1 if (sender or "") == "assistant" else 0
    out.sort(key=lambda m: (
        parse_dt(m.get("created_at") or ""),
        role_weight(m.get("sender")),
        m.get("RowKey") or "",
    ))

    if since_iso:
        try:
            since = parse_utc_dt(since_iso)
            out = [m for m in out if parse_dt(m.get("created_at")) > since]
        except Exception:
            pass

    return out[-limit:]

# No built-in AI reply in this flow; webhook handles processing TEST

# ------------ Routes ------------
@app.before_request
def ensure_tables():
    # Fast maintenance gate first, without touching Azure if possible
    try:
        path = request.path or "/"
        logged_in = bool(session.get("username"))
        # Always allow these paths regardless of maintenance
        allow = (
            path.startswith("/static/") or
            path.startswith("/admin") or
            path.startswith("/logout") or
            not logged_in  # if not logged in, allow (so /login and /register work)
        )
        if not allow:
            maint = get_maintenance()  # uses in-memory first, cached fallback otherwise
            if maint.get("active") and not session.get("is_admin"):
                # Force logout for non-admin users already in-session when maintenance starts.
                if logged_in:
                    session.pop("username", None)
                    session.pop("session_id", None)
                    session["is_admin"] = False
                    flash("You have been logged out because maintenance is active.", "error")
                    return redirect(url_for("login"))
                return (
                    render_template(
                        "error.html",
                        error_title="Maintenance",
                        error_message=maint.get("message") or "We'll be right back.",
                        suggestion="Please check back soon.",
                    ),
                    503,
                )
    except Exception:
        # Never block requests if check fails
        pass

    # Lazily create tables once per process; avoid per-request Azure calls
    global INIT_DONE
    if not INIT_DONE:
        try:
            init_tables()
            INIT_DONE = True
        except Exception as e:
            from flask import make_response
            html = render_template(
                "error.html",
                error_title="Startup Error",
                error_message=str(e),
                suggestion=(
                    "Check Azure credentials (AZURE_STORAGE_CONNECTION_STRING) or service availability."
                ),
            )
            return make_response(html, 500)

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
            session["is_admin"] = True
            session["username"] = username
            session["session_id"] = ADMIN_SESSION_ID
            return redirect(url_for("chat"))
        # Check if account is locked before password verification
        u = get_user(username)
        if u and u.get("locked"):
            flash("Account is locked. Please contact an administrator.", "error")
            return render_template("math_app_login.html", announcement=get_announcement())
        user = authenticate(username, password)
        if user:
            session["username"] = username
            session["session_id"] = user["session_id"]
            session["is_admin"] = False
            return redirect(url_for("chat"))
        flash("Invalid username or password.", "error")
    return render_template("math_app_login.html", announcement=get_announcement())

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/")
def index():
    if session.get("username") and session.get("session_id"):
        return redirect(url_for("chat"))
    return redirect(url_for("login"))


@app.route("/register-interest-contact", methods=["POST"])
def register_interest_contact():
    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip()
    institution = (request.form.get("institution") or "").strip()
    message = (request.form.get("message") or "").strip()
    if not name or not email or not message:
        flash("Name, email, and message are required.", "error")
        return redirect(url_for("login", register="open"))
    try:
        save_contact_request(
            kind="register_interest",
            name=name,
            email=email,
            message=message,
            extra={"institution": institution},
        )
        submit_formspree_request(
            FORMSPREE_REGISTER_ENDPOINT,
            {
                "name": name,
                "email": email,
                "institution": institution,
                "message": message,
                "form_type": "register_interest",
                "_subject": f"Registration request from {name}",
            },
        )
        flash("Registration request submitted. Dr. Rama will review it and contact you.", "success")
    except Exception as e:
        flash(f"Could not submit registration request: {str(e)}", "error")
        return redirect(url_for("login", register="open"))
    return redirect(url_for("login"))


@app.route("/support-contact", methods=["POST"])
def support_contact():
    name = (request.form.get("name") or "").strip()
    email = (request.form.get("email") or "").strip()
    topic = (request.form.get("topic") or "").strip() or "other"
    message = (request.form.get("message") or "").strip()
    if not name or not email or not message:
        flash("Name, email, and issue details are required.", "error")
        return redirect(url_for("login", support="open"))
    try:
        save_contact_request(
            kind="support",
            name=name,
            email=email,
            message=message,
            extra={"topic": topic},
        )
        submit_formspree_request(
            FORMSPREE_SUPPORT_ENDPOINT,
            {
                "name": name,
                "email": email,
                "topic": topic,
                "message": message,
                "form_type": "support",
                "_subject": f"Support request from {name}",
            },
        )
        flash("Support request submitted. We will get back to you soon.", "success")
    except Exception as e:
        flash(f"Could not submit support request: {str(e)}", "error")
        return redirect(url_for("login", support="open"))
    return redirect(url_for("login"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        confirm_password = (request.form.get("confirm_password") or "").strip()

        # Validate inputs
        if not username or not password:
            flash("Username and password are required", "error")
            return render_template("math_app_register.html")

        # Password validation
        if len(password) < 8:
            flash("Password must be at least 8 characters long", "error")
            return render_template("math_app_register.html")

        if not is_strong_password(password):
            flash("Password must include uppercase, lowercase, number and special character", "error")
            return render_template("math_app_register.html")

        if password != confirm_password:
            flash("Passwords do not match", "error")
            return render_template("math_app_register.html")

        # Create user
        try:
            user = create_user(username, password)
        except ValueError as e:
            flash(str(e), "error")
            return render_template("math_app_register.html")

        session["username"] = user["RowKey"]
        session["session_id"] = user["session_id"]
        return redirect(url_for("chat"))

    return render_template("math_app_register.html")

@app.route("/profile/password", methods=["POST"])
def profile_change_password():
    if not session.get("username"):
        return redirect(url_for("login"))
    if session.get("is_admin"):
        flash("Admin credentials are managed separately.", "error")
        return redirect(url_for("admin_dashboard"))

    current_password = (request.form.get("current_password") or "").strip()
    new_password = (request.form.get("new_password") or "").strip()
    confirm_password = (request.form.get("confirm_password") or "").strip()
    username = session.get("username")

    user = get_user(username)
    if not user:
        flash("User account not found.", "error")
        return redirect(url_for("chat"))

    if user.get("password_hash") != hash_pw(current_password):
        flash("Current password is incorrect.", "error")
        return redirect(url_for("chat"))

    if new_password != confirm_password:
        flash("New password and confirmation do not match.", "error")
        return redirect(url_for("chat"))

    if not is_strong_password(new_password):
        flash("New password must include 8+ chars, uppercase, lowercase, number, and special character.", "error")
        return redirect(url_for("chat"))

    if hash_pw(new_password) == user.get("password_hash"):
        flash("New password must be different from current password.", "error")
        return redirect(url_for("chat"))

    try:
        user["password_hash"] = hash_pw(new_password)
        user["updated_at"] = utc_now_iso()
        _table(REGISTER_TABLE).upsert_entity(user)
        flash("Password updated successfully.", "success")
    except Exception as e:
        flash(f"Could not update password: {str(e)}", "error")

    return redirect(url_for("chat"))

# Legacy endpoint placeholder (not used in single-chat design)
@app.route("/chat/create", methods=["POST"])
def create_chat_route():
    return redirect(url_for("chat"))

@app.route("/chat")
def chat():
    if not session.get("username"):
        return redirect(url_for("login"))
    if not session.get("session_id") and session.get("is_admin"):
        session["session_id"] = ADMIN_SESSION_ID
    msgs = list_messages_v2(session["session_id"], limit=200)
    return render_template("chat.html", chat={"name": "Assistant"}, messages=msgs, username=session["username"]) 

@app.route("/chat/ocr", methods=["POST"])
def chat_ocr():
    def _no_store(payload, status=200):
        resp = jsonify(payload)
        resp.status_code = status
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp

    if not session.get("username"):
        return _no_store({"ok": False, "error": "Unauthorized."}, 401)

    upload = request.files.get("image")
    if not upload or not upload.filename:
        return _no_store({"ok": False, "error": "Please attach an image file."}, 400)

    ext = upload.filename.rsplit(".", 1)[-1].lower() if "." in upload.filename else ""
    allowed_ext = {"png", "jpg", "jpeg", "webp", "bmp", "gif", "tif", "tiff"}
    if ext and ext not in allowed_ext:
        return _no_store({"ok": False, "error": "Unsupported image format."}, 415)

    mime = (upload.mimetype or "").lower()
    if mime and not mime.startswith("image/"):
        return _no_store({"ok": False, "error": "Attached file must be an image."}, 415)

    raw = b""
    try:
        raw = upload.read()
    except Exception:
        return _no_store({"ok": False, "error": "Unable to read uploaded file."}, 400)
    finally:
        try:
            upload.close()
        except Exception:
            pass

    if not raw:
        return _no_store({"ok": False, "error": "Image file is empty."}, 400)

    max_bytes = OCR_MAX_FILE_MB * 1024 * 1024
    if len(raw) > max_bytes:
        return _no_store(
            {"ok": False, "error": f"Image is too large. Max size is {OCR_MAX_FILE_MB} MB."},
            413,
        )

    try:
        text = clean_ocr_text(extract_text_from_image_bytes(raw))
        chat_text = format_ocr_text_for_chat(text)
    except RuntimeError as exc:
        return _no_store({"ok": False, "error": str(exc)}, 503)
    except Exception:
        return _no_store({"ok": False, "error": "OCR failed while processing the image."}, 500)
    finally:
        raw = b""

    if not text:
        return _no_store(
            {"ok": False, "error": "No readable text was found in this image."},
            422,
        )

    return _no_store(
        {
            "ok": True,
            "text": text,
            "chat_text": chat_text or text,
            "char_count": len(chat_text or text),
        }
    )

@app.route("/chat/ocr/math", methods=["POST"])
def chat_ocr_math():
    def _no_store(payload, status=200):
        resp = jsonify(payload)
        resp.status_code = status
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp

    if not session.get("username"):
        return _no_store({"ok": False, "error": "Unauthorized."}, 401)

    upload = request.files.get("image")
    if not upload or not upload.filename:
        return _no_store({"ok": False, "error": "Please attach a formula image file."}, 400)

    mime = (upload.mimetype or "").lower()
    if mime and not mime.startswith("image/"):
        return _no_store({"ok": False, "error": "Attached file must be an image."}, 415)

    raw = b""
    try:
        raw = upload.read()
    except Exception:
        return _no_store({"ok": False, "error": "Unable to read uploaded file."}, 400)
    finally:
        try:
            upload.close()
        except Exception:
            pass

    if not raw:
        return _no_store({"ok": False, "error": "Image file is empty."}, 400)

    max_bytes = OCR_MAX_FILE_MB * 1024 * 1024
    if len(raw) > max_bytes:
        return _no_store(
            {"ok": False, "error": f"Image is too large. Max size is {OCR_MAX_FILE_MB} MB."},
            413,
        )

    try:
        latex = clean_ocr_text(extract_formula_latex_from_image_bytes(raw))
    except RuntimeError as exc:
        return _no_store({"ok": False, "error": str(exc)}, 503)
    except Exception:
        return _no_store({"ok": False, "error": "Formula OCR failed while processing the image."}, 500)
    finally:
        raw = b""

    if not latex:
        return _no_store(
            {"ok": False, "error": "No clear formula was detected in this image."},
            422,
        )

    return _no_store({"ok": True, "latex": latex, "char_count": len(latex)})

@app.route("/chat/send", methods=["POST"])
def chat_send():
    wants_json = (request.headers.get("X-Requested-With") or "").lower() == "xmlhttprequest"
    if not session.get("username"):
        if wants_json:
            return jsonify({"ok": False, "error": "Unauthorized."}), 401
        return redirect(url_for("login"))

    text = (request.form.get("text") or "").strip()
    upload = request.files.get("image")
    image_info = None
    image_upload_error = ""
    image_requested = bool(upload and upload.filename)
    if image_requested and not session.get("is_admin"):
        if wants_json:
            return jsonify({"ok": False, "error": "Image attachment is available for admin account only."}), 403
        flash("Image attachment is available for admin account only.", "error")
        return redirect(url_for("chat"))
    image_filename = os.path.basename(str(upload.filename or "")).strip() if image_requested else ""
    if image_requested:
        try:
            image_info = upload_chat_image_to_blob(upload, session.get("session_id"))
            image_filename = str(image_info.get("filename") or image_filename).strip() or image_filename
        except Exception as exc:
            image_upload_error = str(exc)
            flash(f"Image upload warning: {image_upload_error}", "error")

    if not text and not image_requested:
        if wants_json:
            return jsonify({"ok": False, "error": "Message or image is required."}), 400
        return redirect(url_for("chat"))

    display_text = text if text else ""
    message_type = "text"
    if image_requested and display_text:
        message_type = "text_image"
    elif image_requested:
        message_type = "image_only"
    attachment = None
    if image_info:
        attachment = {
            "image_url": image_info.get("blob_url"),
            "image_filename": image_filename,
            "image_container": image_info.get("container"),
            "image_blob_name": image_info.get("blob_name"),
            "image_mime": image_info.get("mime"),
            "image_size": image_info.get("size"),
        }
    # Save the user message in Azure Table
    msg = post_message(session["session_id"], "user", display_text, attachment=attachment)
    # Send to n8n in the requested array/Telegram-like structure
    try:
        ts = int(datetime.now(timezone.utc).timestamp())
        user_id = int(session.get("session_id", "0")) if str(session.get("session_id", "0")).isdigit() else 0
        meta = {
            "session_id": session.get("session_id"),
            "row_key": msg.get("RowKey"),
            "created_at": to_utc_iso_z(msg.get("created_at")) or utc_now_iso(),
            "message_type": message_type,
            "image_sent": "yes" if image_requested else "no",
            "image_filename": image_filename if image_requested else "",
        }
        if image_info:
            meta["image_url"] = image_info.get("blob_url")
            meta["image_container"] = image_info.get("container")
            meta["image_blob_name"] = image_info.get("blob_name")
            meta["image_mime"] = image_info.get("mime")
            meta["image_size"] = image_info.get("size")
            try:
                meta["image_preview_url"] = url_for("chat_image_proxy", row_key=msg.get("RowKey"))
            except Exception:
                pass
            meta["image_upload"] = "success"
        elif image_requested:
            meta["image_upload"] = "failed"
            meta["image_error"] = image_upload_error or "Image upload failed."

        payload = [
            {
                "update_id": random.randint(100000000, 999999999),
                "message": {
                    "message_id": random.randint(1, 1000000000),
                    "from": {
                        "id": user_id,
                        "is_bot": False,
                        "first_name": session.get("username", ""),
                        "last_name": "",
                        "username": session.get("username", ""),
                        "language_code": "en",
                    },
                    "chat": {
                        "id": user_id,
                        "first_name": session.get("username", ""),
                        "last_name": "",
                        "username": session.get("username", ""),
                        "type": "private",
                    },
                    "date": ts,
                    "text": display_text,
                    "message_type": message_type,
                    "image_filename": image_filename if image_requested else "",
                    "image_url": image_info.get("blob_url") if image_info else "",
                    "image_preview_url": (url_for("chat_image_proxy", row_key=msg.get("RowKey")) if image_info else ""),
                    # Extra metadata (not in Telegram spec) to help your flow
                    "_meta": meta,
                },
            }
        ]
        requests.post(get_webhook_url(), json=payload, timeout=8)
    except Exception:
        # Don't block on webhook errors
        pass
    if wants_json:
        return jsonify(
            {
                "ok": True,
                "row_key": msg.get("RowKey"),
                "image_requested": image_requested,
                "image_uploaded": bool(image_info),
                "image_error": image_upload_error,
            }
        )
    return redirect(url_for("chat"))

@app.route("/chat/clear", methods=["POST"])
def chat_clear():
    if not session.get("username"):
        return redirect(url_for("login"))
    try:
        tbl = _table(CHATS_TABLE)
        sid = session.get("session_id")
        delete_fields = ["PartitionKey", "RowKey", "sendto"]
        # Delete all user messages for this session
        user_entities = list(tbl.query_entities(f"PartitionKey eq '{sid}'", select=delete_fields))
        for ent in user_entities:
            try:
                tbl.delete_entity(partition_key=ent["PartitionKey"], row_key=ent["RowKey"])
            except ResourceNotFoundError:
                flash(f"Message with RowKey {ent['RowKey']} not found.", "error")
            except Exception as e:
                flash(f"Error deleting message: {str(e)}", "error")
        
        # Delete all AI responses addressed to this session
        ai_entities = list(
            tbl.query_entities(
                f"PartitionKey eq 'AI_{sid}' or sendto eq 'session_{sid}'",
                select=delete_fields,
            )
        )
        for ent in ai_entities:
            try:
                tbl.delete_entity(partition_key=ent["PartitionKey"], row_key=ent["RowKey"])
            except ResourceNotFoundError:
                flash(f"AI response with RowKey {ent['RowKey']} not found.", "error")
            except Exception as e:
                flash(f"Error deleting AI response: {str(e)}", "error")
        
        if user_entities or ai_entities:
            flash("All messages and AI responses cleared.", "success")
        else:
            flash("No messages or AI responses found to clear.", "info")
    except Exception as e:
        flash(f"Failed to clear messages: {str(e)}", "error")
    # Also call n8n webhook to purge external DB rows by session_id
    try:
        sid = session.get("session_id")
        if sid:
            # Webhook expects GET; passes session_id for downstream deletion
            requests.get(N8N_DELETE_WEBHOOK, params={"session_id": sid}, timeout=8)
    except Exception:
        # Non-blocking; UI still clears even if webhook unavailable
        pass
    return jsonify(success=True)

@app.route("/chat/messages")
def chat_messages():
    if not session.get("username"):
        resp = jsonify([])
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        return resp
    since = stringify_ts(request.args.get("since"))
    msgs = list_messages_v2(session["session_id"], since_iso=since, limit=200)
    resp = jsonify(msgs)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp

@app.route("/chat/image/<row_key>")
def chat_image_proxy(row_key):
    if not session.get("username"):
        return Response(status=401)
    sid = session.get("session_id")
    if not sid or not row_key:
        return Response(status=404)
    try:
        ent = _table(CHATS_TABLE).get_entity(partition_key=sid, row_key=row_key)
    except Exception:
        return Response(status=404)

    blob_name = str(ent.get("image_blob_name") or "").strip()
    if not blob_name:
        return Response(status=404)
    container_name = str(ent.get("image_container") or CHAT_IMAGE_CONTAINER).strip() or CHAT_IMAGE_CONTAINER
    image_mime = str(ent.get("image_mime") or "").strip() or "application/octet-stream"
    try:
        if container_name == LOCAL_CHAT_IMAGE_CONTAINER:
            raw = _read_chat_image_locally(blob_name)
        elif BLOB_AVAILABLE:
            blob = _blob_container(container_name).get_blob_client(blob_name)
            raw = blob.download_blob().readall()
        else:
            raw = _download_blob_via_rest(
                conn_str=_blob_conn_str(),
                container_name=container_name,
                blob_name=blob_name,
            )
    except Exception:
        return Response(status=404)
    resp = Response(raw, mimetype=image_mime)
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp


@app.route("/changefeed/image/<row_key>")
def changefeed_image_proxy(row_key):
    if not session.get("username"):
        return Response(status=401)
    ent = get_changefeed_item(row_key)
    if not ent:
        return Response(status=404)
    blob_name = str(ent.get("image_blob_name") or "").strip()
    if not blob_name:
        return Response(status=404)
    container_name = str(ent.get("image_container") or CHAT_IMAGE_CONTAINER).strip() or CHAT_IMAGE_CONTAINER
    image_mime = str(ent.get("image_mime") or "").strip() or "application/octet-stream"
    try:
        if container_name == LOCAL_CHAT_IMAGE_CONTAINER:
            raw = _read_chat_image_locally(blob_name)
        elif BLOB_AVAILABLE:
            blob = _blob_container(container_name).get_blob_client(blob_name)
            raw = blob.download_blob().readall()
        else:
            raw = _download_blob_via_rest(
                conn_str=_blob_conn_str(),
                container_name=container_name,
                blob_name=blob_name,
            )
    except Exception:
        return Response(status=404)
    resp = Response(raw, mimetype=image_mime)
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp


@app.route("/changes")
def changefeed():
    if not session.get("username"):
        return redirect(url_for("login"))
    items = list_changefeed_items(limit=80)
    return render_template("change_feed.html", title="Change Feed", items=items)


@app.route("/chat/sse")
def chat_sse():
    if not session.get("username"):
        return Response("", mimetype="text/event-stream")

    # Start from 'since' query or last page render time hint
    since = stringify_ts(request.args.get("since"))
    # Support reconnection via Last-Event-ID header (format: created_at|rowkey)
    last_event_id = request.headers.get("Last-Event-ID")
    if (not since) and last_event_id:
        try:
            since = stringify_ts(last_event_id.split("|")[0])
        except Exception:
            pass

    @stream_with_context
    def event_stream():
        nonlocal since
        # Send a quick open event so the client knows we're connected
        yield "event: ping\n" + f"data: {{\"ok\":true}}\n\n"
        sent = set()
        while True:
            try:
                # If maintenance turns on while the stream is already open,
                # signal and close so the client reconnects into the logout flow.
                if not session.get("is_admin"):
                    maint = get_maintenance()
                    if maint.get("active"):
                        yield "event: maintenance\ndata: {}\n\n"
                        break
                since_arg = stringify_ts(since)
                items = list_messages_v2(session["session_id"], since_iso=since_arg, limit=200)
                if items:
                    # Emit one event per message to keep UI simple
                    for it in items:
                        rk = it.get('RowKey') or ''
                        if rk in sent:
                            continue
                        created_at = to_utc_iso_z(it.get("created_at")) or utc_now_iso()
                        it["created_at"] = created_at
                        payload = json.dumps(it, separators=(",", ":"), default=json_dt_default)
                        ev_id = f"{str(created_at)}|{rk}"
                        yield f"id: {ev_id}\n" + "data: " + payload + "\n\n"
                        sent.add(rk)
                    since = stringify_ts(items[-1].get("created_at"))
                else:
                    # heartbeat to keep connection alive
                    yield ": keep-alive\n\n"
            except Exception:
                # On any error, emit a retry hint and continue
                yield "event: error\n" + "data: {}\n\n"
            # Gentle sleep to avoid hammering storage; ~500ms
            import time
            time.sleep(0.5)

    headers = {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "X-Accel-Buffering": "no",  # for proxies like nginx
    }
    return Response(event_stream(), mimetype="text/event-stream", headers=headers)

# Admin routes
@app.route("/admin", methods=["GET"])
def admin_dashboard():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    all_users = list(_table(REGISTER_TABLE).list_entities())
    for user in all_users:
        user["locked"] = user.get("locked", False)  # Ensure locked status is included
    all_users.sort(key=lambda u: (u.get("RowKey") or "").lower())
    q = (request.args.get("q") or "").strip()
    show_all = (request.args.get("show") or "").strip().lower() == "all"
    if q:
        ql = q.lower()
        users_filtered = [
            u for u in all_users
            if ql in (u.get("RowKey") or "").lower()
            or ql in str(u.get("session_id") or "").lower()
        ]
    else:
        users_filtered = all_users
    default_user_limit = 10
    users = users_filtered if show_all else users_filtered[:default_user_limit]

    ann = get_config_cached("announcement") or {}
    maint = get_config_cached("maintenance") or {}

    ann_title = ann.get("title") if isinstance(ann, dict) else getattr(ann, "title", None)
    ann_message = ann.get("message") if isinstance(ann, dict) else getattr(ann, "message", None)
    ann_level = ann.get("level") if isinstance(ann, dict) else getattr(ann, "level", None)
    ann_active = bool(ann.get("active")) if isinstance(ann, dict) else bool(getattr(ann, "active", False))
    ann_scheduled_enabled = _to_bool(ann.get("scheduled_enabled")) if isinstance(ann, dict) else _to_bool(getattr(ann, "scheduled_enabled", False))
    ann_recurring_enabled = _to_bool(ann.get("recurring_enabled")) if isinstance(ann, dict) else _to_bool(getattr(ann, "recurring_enabled", False))
    ann_weekdays = parse_weekdays_field(ann.get("weekdays")) if isinstance(ann, dict) else parse_weekdays_field(getattr(ann, "weekdays", []))
    ann_recurring_start_time = (ann.get("recurring_start_time") if isinstance(ann, dict) else getattr(ann, "recurring_start_time", None)) or ""
    ann_recurring_end_time = (ann.get("recurring_end_time") if isinstance(ann, dict) else getattr(ann, "recurring_end_time", None)) or ""
    ann_start_at = to_utc_iso_z(ann.get("start_at")) if isinstance(ann, dict) and ann.get("start_at") else to_utc_iso_z(getattr(ann, "start_at", None))
    ann_end_at = to_utc_iso_z(ann.get("end_at")) if isinstance(ann, dict) and ann.get("end_at") else to_utc_iso_z(getattr(ann, "end_at", None))

    maint_active = bool(maint.get("active")) if isinstance(maint, dict) else bool(getattr(maint, "active", False))
    maint_message = (maint.get("message") if isinstance(maint, dict) else getattr(maint, "message", None)) or "The system is under maintenance."
    maint_scheduled_enabled = _to_bool(maint.get("scheduled_enabled")) if isinstance(maint, dict) else _to_bool(getattr(maint, "scheduled_enabled", False))
    maint_recurring_enabled = _to_bool(maint.get("recurring_enabled")) if isinstance(maint, dict) else _to_bool(getattr(maint, "recurring_enabled", False))
    maint_weekdays = parse_weekdays_field(maint.get("weekdays")) if isinstance(maint, dict) else parse_weekdays_field(getattr(maint, "weekdays", []))
    maint_recurring_start_time = (maint.get("recurring_start_time") if isinstance(maint, dict) else getattr(maint, "recurring_start_time", None)) or ""
    maint_recurring_end_time = (maint.get("recurring_end_time") if isinstance(maint, dict) else getattr(maint, "recurring_end_time", None)) or ""
    maint_start_at = to_utc_iso_z(maint.get("start_at")) if isinstance(maint, dict) and maint.get("start_at") else to_utc_iso_z(getattr(maint, "start_at", None))
    maint_end_at = to_utc_iso_z(maint.get("end_at")) if isinstance(maint, dict) and maint.get("end_at") else to_utc_iso_z(getattr(maint, "end_at", None))

    ctx = {
        "users": users,
        "all_users_count": len(all_users),
        "filtered_users_count": len(users_filtered),
        "show_all_users": show_all,
        "default_user_limit": default_user_limit,
        "user_query": q,
        "has_more_users": (len(users_filtered) > default_user_limit) and not show_all,
        "username_suggestions": [u.get("RowKey") for u in all_users[:250] if u.get("RowKey")],
        # Announcement fields with safe defaults
        "ann_title": ann_title,
        "ann_message": ann_message,
        "ann_level": ann_level,
        "ann_active": ann_active,
        "ann_scheduled_enabled": ann_scheduled_enabled,
        "ann_recurring_enabled": ann_recurring_enabled,
        "ann_weekdays": ann_weekdays,
        "ann_recurring_start_time": ann_recurring_start_time,
        "ann_recurring_end_time": ann_recurring_end_time,
        "ann_start_at": ann_start_at,
        "ann_end_at": ann_end_at,
        # Maintenance fields
        "maint_active": maint_active,
        "maint_message": maint_message,
        "maint_scheduled_enabled": maint_scheduled_enabled,
        "maint_recurring_enabled": maint_recurring_enabled,
        "maint_weekdays": maint_weekdays,
        "maint_recurring_start_time": maint_recurring_start_time,
        "maint_recurring_end_time": maint_recurring_end_time,
        "maint_start_at": maint_start_at,
        "maint_end_at": maint_end_at,
        # Webhook
        "webhook_env": get_webhook_env(),
        "webhook_test_url": N8N_TEST_WEBHOOK,
        "webhook_prod_url": N8N_PROD_WEBHOOK,
        "webhook_current_url": get_webhook_url(),
        "admin_now_utc": utc_now_iso(),
        "changefeed_items": list_changefeed_items(limit=60),
    }
    return render_template("admin.html", **ctx)


@app.route("/admin/changefeed/create", methods=["POST"])
def admin_changefeed_create():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    title = (request.form.get("title") or "").strip()
    body = (request.form.get("body") or "").strip()
    upload = request.files.get("image")
    image_info = None
    try:
        if upload and upload.filename:
            image_info = upload_changefeed_image_to_blob(upload)
        create_changefeed_item(title=title, body=body, image_info=image_info)
        flash("Change feed entry created.", "success")
    except Exception as exc:
        flash("Failed to create change feed entry: " + str(exc), "error")
    return redirect(url_for("admin_dashboard") + "#changefeed")


@app.route("/admin/changefeed/delete", methods=["POST"])
def admin_changefeed_delete():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    row_key = (request.form.get("row_key") or "").strip()
    try:
        if not row_key:
            raise ValueError("Missing row key.")
        ok = delete_changefeed_item(row_key)
        if ok:
            flash("Change feed entry deleted.", "success")
        else:
            flash("Change feed entry not found.", "error")
    except Exception as exc:
        flash("Failed to delete change feed entry: " + str(exc), "error")
    return redirect(url_for("admin_dashboard") + "#changefeed")

@app.route("/admin/create_account", methods=["POST"])
def admin_create_account():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    username = request.form.get("username")
    password = request.form.get("password")
    confirm_password = request.form.get("confirm_password")
    try:
        # Basic validations mirroring register()
        if not username or not password:
            raise ValueError("Username and password are required")
        if password != (confirm_password or ""):
            raise ValueError("Passwords do not match")
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters long")
        if not (any(c.isupper() for c in password) and any(c.islower() for c in password) and any(c.isdigit() for c in password) and any(c in '@$!%*?&' for c in password)):
            raise ValueError("Password must include uppercase, lowercase, number and special character")
        create_user(username, password)
        flash("Account created successfully.", "success")
    except ValueError as e:
        flash(str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/lock_account", methods=["POST"])
def admin_lock_account():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    username = request.form.get("username")
    tbl = _table(REGISTER_TABLE)
    try:
        user = get_user(username)
        if user:
            user["locked"] = True
            tbl.upsert_entity(user)
            flash("Account locked successfully.", "success")
        else:
            flash("User not found.", "error")
    except Exception as e:
        flash("Failed to lock account: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/reset_password", methods=["POST"])
def admin_reset_password():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    username = (request.form.get("username") or "").strip()
    new_password = (request.form.get("new_password") or "").strip()
    tbl = _table(REGISTER_TABLE)
    try:
        if not username:
            raise ValueError("Username is required.")
        if not new_password:
            raise ValueError("New password is required.")
        if len(new_password) < 8:
            raise ValueError("Password must be at least 8 characters long.")
        if not is_strong_password(new_password):
            raise ValueError("Password must include uppercase, lowercase, number and special character (@$!%*?&).")
        user = get_user(username)
        if user:
            user["password_hash"] = hash_pw(new_password)
            tbl.upsert_entity(user)
            flash("Password reset successfully.", "success")
        else:
            flash("User not found.", "error")
    except Exception as e:
        flash("Failed to reset password: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/delete_account", methods=["POST"])
def admin_delete_account():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    username = request.form.get("username")
    try:
        # Delete user account
        tbl = _table(REGISTER_TABLE)
        user = get_user(username)
        if user:
            tbl.delete_entity(partition_key="USER", row_key=username)

            # Delete associated chats
            chat_tbl = _table(CHATS_TABLE)
            session_id = user.get("session_id")
            chat_entities = list(
                chat_tbl.query_entities(
                    f"PartitionKey eq '{session_id}'",
                    select=["PartitionKey", "RowKey"],
                )
            )
            for chat in chat_entities:
                chat_tbl.delete_entity(partition_key=session_id, row_key=chat.get("RowKey"))

            flash("Account and associated chats deleted successfully.", "success")
        else:
            flash("User not found.", "error")
    except Exception as e:
        flash("Failed to delete account: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/announcement/save", methods=["POST"])
def admin_save_announcement():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    title = (request.form.get("title") or "").strip()
    message = (request.form.get("message") or "").strip()
    level = (request.form.get("level") or "info").strip()
    active = bool(request.form.get("active"))
    scheduled_enabled = bool(request.form.get("scheduled_enabled"))
    recurring_enabled = bool(request.form.get("recurring_enabled"))
    weekdays = parse_weekdays_field(request.form.getlist("weekdays"))
    weekdays_storage = ",".join(str(d) for d in weekdays)
    recurring_start_time = (request.form.get("recurring_start_time") or "").strip()
    recurring_end_time = (request.form.get("recurring_end_time") or "").strip()
    tz_offset_minutes = _to_int(request.form.get("tz_offset_minutes"), 0)
    tz_name = normalize_timezone_name(request.form.get("tz_name")) or normalize_timezone_name(get_admin_preferences().get("timezone")) or "UTC"
    start_at = parse_local_datetime_to_utc_iso(request.form.get("start_at"), tz_offset_minutes, tz_name=tz_name)
    end_at = parse_local_datetime_to_utc_iso(request.form.get("end_at"), tz_offset_minutes, tz_name=tz_name)
    try:
        if scheduled_enabled and not recurring_enabled and not (start_at or end_at):
            raise ValueError("Set start/end date-time, or disable schedule window.")
        if start_at and end_at and parse_utc_dt(end_at) <= parse_utc_dt(start_at):
            raise ValueError("Announcement end time must be after start time.")
        if recurring_enabled and not weekdays:
            raise ValueError("Select at least one weekday, or disable weekly recurrence.")
        if recurring_enabled:
            if parse_hhmm(recurring_start_time) is None and recurring_start_time:
                raise ValueError("Invalid recurring start time.")
            if parse_hhmm(recurring_end_time) is None and recurring_end_time:
                raise ValueError("Invalid recurring end time.")
        global ANNOUNCEMENT_STATE
        ANNOUNCEMENT_STATE = {
            "title": title or "Announcement",
            "message": message,
            "level": level,
            "active": active,
            "scheduled_enabled": scheduled_enabled,
            "recurring_enabled": recurring_enabled,
            "weekdays": weekdays_storage,
            "tz_offset_minutes": tz_offset_minutes,
            "tz_name": tz_name,
            "recurring_start_time": recurring_start_time,
            "recurring_end_time": recurring_end_time,
            "start_at": start_at,
            "end_at": end_at,
        }
        set_config("announcement", ANNOUNCEMENT_STATE)
        flash("Announcement saved.", "success")
    except Exception as e:
        flash("Failed to save announcement: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/unlock_account", methods=["POST"])
def admin_unlock_account():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    username = request.form.get("username")
    tbl = _table(REGISTER_TABLE)
    try:
        user = get_user(username)
        if user:
            user["locked"] = False
            tbl.upsert_entity(user)
            flash("Account unlocked.", "success")
        else:
            flash("User not found.", "error")
    except Exception as e:
        flash("Failed to unlock account: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/announcement/clear", methods=["POST"])
def admin_clear_announcement():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    try:
        global ANNOUNCEMENT_STATE
        ANNOUNCEMENT_STATE = None
        delete_config("announcement")
        flash("Announcement cleared.", "success")
    except Exception as e:
        flash("Failed to clear announcement: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/maintenance/save", methods=["POST"])
def admin_save_maintenance():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    active = bool(request.form.get("active"))
    message = (request.form.get("message") or "").strip() or "The system is under maintenance."
    scheduled_enabled = bool(request.form.get("scheduled_enabled"))
    recurring_enabled = bool(request.form.get("recurring_enabled"))
    weekdays = parse_weekdays_field(request.form.getlist("weekdays"))
    weekdays_storage = ",".join(str(d) for d in weekdays)
    recurring_start_time = (request.form.get("recurring_start_time") or "").strip()
    recurring_end_time = (request.form.get("recurring_end_time") or "").strip()
    tz_offset_minutes = _to_int(request.form.get("tz_offset_minutes"), 0)
    tz_name = normalize_timezone_name(request.form.get("tz_name")) or normalize_timezone_name(get_admin_preferences().get("timezone")) or "UTC"
    start_at = parse_local_datetime_to_utc_iso(request.form.get("start_at"), tz_offset_minutes, tz_name=tz_name)
    end_at = parse_local_datetime_to_utc_iso(request.form.get("end_at"), tz_offset_minutes, tz_name=tz_name)
    try:
        if scheduled_enabled and not recurring_enabled and not (start_at or end_at):
            raise ValueError("Set start/end date-time, or disable schedule window.")
        if start_at and end_at and parse_utc_dt(end_at) <= parse_utc_dt(start_at):
            raise ValueError("Maintenance end time must be after start time.")
        if recurring_enabled and not weekdays:
            raise ValueError("Select at least one weekday, or disable weekly recurrence.")
        if recurring_enabled:
            if parse_hhmm(recurring_start_time) is None and recurring_start_time:
                raise ValueError("Invalid recurring start time.")
            if parse_hhmm(recurring_end_time) is None and recurring_end_time:
                raise ValueError("Invalid recurring end time.")
        # Update fast in-memory copy for immediate effect
        global MAINTENANCE_STATE
        MAINTENANCE_STATE = {
            "active": active,
            "message": message,
            "scheduled_enabled": scheduled_enabled,
            "recurring_enabled": recurring_enabled,
            "weekdays": weekdays_storage,
            "tz_offset_minutes": tz_offset_minutes,
            "tz_name": tz_name,
            "recurring_start_time": recurring_start_time,
            "recurring_end_time": recurring_end_time,
            "start_at": start_at,
            "end_at": end_at,
        }
        set_config("maintenance", MAINTENANCE_STATE)
        flash("Maintenance settings updated.", "success")
    except Exception as e:
        flash("Failed to update maintenance settings: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/error-preview")
def admin_error_preview():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    ann = get_config_cached("announcement") or {}
    maint = get_config_cached("maintenance") or {}
    title = ann.get("title") or "Preview Error"
    msg = maint.get("message") or ann.get("message") or "Example preview of the error/maintenance page."
    return render_template("error.html", error_title=title, error_message=msg, suggestion="This is a preview for admins only."), 200

@app.route("/admin/webhook/save", methods=["POST"])
def admin_save_webhook():
    if not session.get("is_admin"):
        return redirect(url_for("login"))
    env = (request.form.get("env") or "test").strip().lower()
    try:
        set_webhook_env(env)
        flash(f"Webhook environment set to {env}.", "success")
    except Exception as e:
        flash("Failed to save webhook preference: " + str(e), "error")
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/preferences/timezone", methods=["POST"])
def admin_save_timezone_preference():
    if not session.get("is_admin"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401
    tz_name = ""
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        tz_name = (payload.get("timezone") or "").strip()
    else:
        tz_name = (request.form.get("timezone") or "").strip()
    try:
        saved = set_admin_timezone_preference(tz_name)
        return jsonify({"ok": True, "timezone": saved})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

# Legacy chat management routes removed in this flow

# ------------ Error handlers ------------
@app.errorhandler(Exception)
def handle_any_exception(e):
    # Let HTTPException (404, 405, etc.) render default pages
    if isinstance(e, HTTPException):
        return e
    title = "Unexpected Error"
    msg = str(e)
    return (
        render_template(
            "error.html",
            error_title=title,
            error_message=msg,
            suggestion="Please retry or contact support if it persists.",
        ),
        500,
    )

try:
    # Import lazily so the app still runs if azure libs are unavailable during tooling
    from azure.core.exceptions import ClientAuthenticationError

    @app.errorhandler(ClientAuthenticationError)
    def handle_azure_auth_error(e):
        return (
            render_template(
                "error.html",
                error_title="Azure Authentication Failed",
                error_message=(
                    "Server failed to authenticate the request. Check the connection string or SAS token."
                ),
                suggestion=(
                    "Verify AZURE_STORAGE_CONNECTION_STRING (ensure AccountKey or SharedAccessSignature is present and correct)."
                ),
            ),
            500,
        )
except Exception:
    # If azure library import fails, the generic handler above will cover errors.
    pass


@app.errorhandler(404)
def handle_404(e):
    return (
        render_template(
            "error.html",
            error_title="Not Found",
            error_message="The requested page was not found.",
            suggestion="Check the URL or return to the homepage.",
        ),
        404,
    )

@app.errorhandler(500)
def handle_500(e):
    return (
        render_template(
            "error.html",
            error_title="Server Error",
            error_message="An internal error occurred.",
            suggestion="Please try again later.",
        ),
        500,
    )

if __name__ == "__main__":
    # Threaded to ensure the SSE stream does not block other requests
    debug = os.getenv("FLASK_DEBUG", "0") in {"1", "true", "True"}
    app.run(debug=debug, host="0.0.0.0", port=int("5000"), threaded=True)
