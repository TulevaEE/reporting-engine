"""Metabase'i aruandekaardid Tuleva agent gateway kaudu.

Isiklikku Metabase'i võtit enam ei ole (sisekord 22 p 4.1). Gateway tööriist
``reports`` annab välja ainult Monthly KPIs (74) ja Weekly (12) dashboardi
kaarte, ja iga päring logitakse sinu nimel (``audit_trail``).

Sisselogimine: esimesel korral avab skript brauseri, logid sisse oma Tuleva
Google'i kontoga ja kinnitad nõusoleku. Tokenid jäävad faili
``~/.cache/tuleva-reports/gateway-oauth.json`` (õigused 600, väljaspool repot).
Refresh token kehtib nädala, siis küsitakse uuesti sisselogimist.

Liides on sama mis ``MetabaseClient.execute_card``-il: kaardi read
sõnastike nimekirjana, veeru nimi võtmeks. Gateway annab väärtused tekstina
(Metabase'i CSV-eksport); siin teisendatakse täisarvud ja murdarvud arvudeks
ja tühi lahter ``None``-iks, kuupäevad jäävad tekstiks.

    from gateway_client import GatewayClient
    rows = GatewayClient().execute_card(2578)
"""
import asyncio
import json
import os
import re
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from mcp import ClientSession
from mcp.client.auth import OAuthClientProvider
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.auth import (AuthorizationCodeResult, OAuthClientInformationFull,
                             OAuthClientMetadata, OAuthToken)

GATEWAY_URL = os.environ.get('TULEVA_GATEWAY_URL', 'https://agent-gateway.tuleva.ee/mcp')
CALLBACK_PORT = int(os.environ.get('TULEVA_GATEWAY_CALLBACK_PORT', '8765'))
CALLBACK_PATH = '/callback'
TOKEN_FILE = Path(os.environ.get('TULEVA_CACHE_DIR', Path.home() / '.cache' / 'tuleva-reports')) \
    / 'gateway-oauth.json'
TOOL = 'reports'
CALL_TIMEOUT_SECONDS = 300
LOGIN_TIMEOUT_SECONDS = 300

_INTEGER = re.compile(r'-?\d+')
_DECIMAL = re.compile(r'-?(\d+\.\d*|\.\d+|\d+)([eE][-+]?\d+)?')


class GatewayRefused(RuntimeError):
    """Gateway vastas keeldumisega (nt kaart ei ole dashboardil)."""


class _FileTokenStorage:
    """OAuth klient ja tokenid ühes failis, mida loeb ja kirjutab ainult kasutaja ise."""

    def __init__(self, path: Path):
        self.path = path

    def _read(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except FileNotFoundError:
            return {}

    def _write(self, key: str, value: dict) -> None:
        stored = self._read()
        stored[key] = value
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w') as f:
            json.dump(stored, f)

    async def get_tokens(self):
        stored = self._read().get('tokens')
        return OAuthToken.model_validate(stored) if stored else None

    async def set_tokens(self, tokens) -> None:
        self._write('tokens', tokens.model_dump(mode='json', exclude_none=True))

    async def get_client_info(self):
        stored = self._read().get('client_info')
        return OAuthClientInformationFull.model_validate(stored) if stored else None

    async def set_client_info(self, client_info) -> None:
        self._write('client_info', client_info.model_dump(mode='json', exclude_none=True))


def _wait_for_callback() -> AuthorizationCodeResult:
    """Ootab, kuni brauser suunab sisselogimise järel tagasi 127.0.0.1-le."""
    received = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlparse(self.path)
            if url.path != CALLBACK_PATH:
                self.send_response(404)
                self.end_headers()
                return
            query = parse_qs(url.query)
            received['code'] = query.get('code', [None])[0]
            received['state'] = query.get('state', [None])[0]
            received['error'] = query.get('error', [None])[0]
            received['iss'] = query.get('iss', [None])[0]
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write('Sisselogimine õnnestus, võid selle akna sulgeda.'.encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(('127.0.0.1', CALLBACK_PORT), Handler)
    server.timeout = LOGIN_TIMEOUT_SECONDS
    try:
        while 'code' not in received and 'error' not in received:
            server.handle_request()
            if not received:
                raise TimeoutError('Sisselogimine ei jõudnud tagasi '
                                   f'{LOGIN_TIMEOUT_SECONDS} sekundi jooksul')
    finally:
        server.server_close()
    if received.get('error') or not received.get('code'):
        raise RuntimeError(f'Gateway sisselogimine ebaõnnestus: {received.get("error")}')
    return AuthorizationCodeResult(code=received['code'], state=received['state'],
                                   iss=received['iss'])


async def _open_browser(url: str) -> None:
    print(f"Logi gateway'sse sisse brauseris (kui aken ei avanenud, ava see link):\n  {url}")
    webbrowser.open(url)


async def _callback() -> AuthorizationCodeResult:
    return await asyncio.to_thread(_wait_for_callback)


def _value(text):
    if text is None or text == '':
        return None
    if _INTEGER.fullmatch(text):
        return int(text)
    if _DECIMAL.fullmatch(text):
        return float(text)
    return text


class GatewayClient:

    def __init__(self, url: str = GATEWAY_URL, token_file: Path = TOKEN_FILE):
        self.url = url
        self.auth = OAuthClientProvider(
            server_url=url,
            client_metadata=OAuthClientMetadata(
                client_name='tuleva-reports',
                redirect_uris=[f'http://127.0.0.1:{CALLBACK_PORT}{CALLBACK_PATH}'],
                grant_types=['authorization_code', 'refresh_token'],
                response_types=['code'],
                token_endpoint_auth_method='none',
                scope='tuleva:read',
            ),
            storage=_FileTokenStorage(token_file),
            redirect_handler=_open_browser,
            callback_handler=_callback,
        )

    async def _call(self, arguments: dict, tool: str = TOOL) -> dict:
        async with create_mcp_http_client(auth=self.auth) as http:
            async with streamable_http_client(self.url, http_client=http) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(
                        tool, arguments, read_timeout_seconds=CALL_TIMEOUT_SECONDS)
        if result.is_error:
            text = ' '.join(getattr(c, 'text', '') for c in result.content)
            raise RuntimeError(f'Gateway tööriist {tool} ebaõnnestus: {text}')
        answer = result.structured_content
        if answer is None:
            text = ' '.join(getattr(c, 'text', '') for c in result.content)
            return {'text': text}
        if 'refused' in answer:
            raise GatewayRefused(answer['refused'])
        return answer

    def ping(self) -> str:
        """Ühenduse ja sisselogimise kontroll: gateway vastab 'pong (caller: ...)'."""
        answer = asyncio.run(self._call({'message': 'tuleva-reports'}, tool='ping'))
        return answer.get('text') or json.dumps(answer)

    def cards(self) -> list[dict]:
        """Dashboardi kaardid: id ja nimi, ilma väärtusteta."""
        return asyncio.run(self._call({'operation': 'cards'}))['cards']

    def execute_card(self, card_id: int) -> list[dict]:
        answer = asyncio.run(self._call({'operation': 'rows', 'CARD_ID': card_id}))
        columns = answer['columns']
        return [dict(zip(columns, (_value(v) for v in row))) for row in answer['rows']]


if __name__ == '__main__':
    import sys
    client = GatewayClient()
    if sys.argv[1:] == ['ping']:
        print(client.ping())
    else:
        for card in client.cards():
            print(card)
