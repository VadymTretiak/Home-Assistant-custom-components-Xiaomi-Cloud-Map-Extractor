# -*- coding: utf-8 -*-
"""
MIoT http and Pub/Sub client.
"""
import asyncio
import base64
import hashlib
import json
import logging
import random
import re
import ssl
import struct
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Optional, final, Coroutine
from urllib.parse import urlencode

import aiohttp
from paho.mqtt.client import (
    MQTT_ERR_SUCCESS,
    MQTT_ERR_NO_CONN,
    MQTT_ERR_UNKNOWN,
    Client,
    MQTTv5,
    MQTTMessage)

_LOGGER = logging.getLogger(__name__)

# region STUBBED_DEPS
# These are placeholder implementations for dependencies that were not fully provided.

MIHOME_APP_ID = '2882303761517542183'
UNSUPPORTED_MODELS = []
MIHOME_MQTT_KEEPALIVE = 60
DEFAULT_OAUTH2_API_HOST = 'account.xiaomi.com'
MIHOME_HTTP_API_TIMEOUT = 10
OAUTH2_AUTH_URL = 'https://account.xiaomi.com/oauth2/authorize'
TOKEN_EXPIRES_TS_RATIO = 0.7


class MIoTError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class MIoTMipsError(MIoTError):
    pass


class MIoTHttpError(MIoTError):
    pass


class MIoTOauthError(MIoTError):
    pass


class MIoTErrorCode(Enum):
    CODE_INTERNAL_ERROR = -1
    CODE_MIPS_INVALID_RESULT = -2
    CODE_OAUTH_UNAUTHORIZED = -3
    CODE_HTTP_INVALID_ACCESS_TOKEN = -4


def calc_group_id(uid: str, home_id: str) -> str:
    return f"group.{uid}.{home_id}"


class MIoTMatcher:
    def __init__(self):
        self._subscriptions = {}

    def __setitem__(self, topic, value):
        self._subscriptions[topic] = value

    def __getitem__(self, topic):
        return self._subscriptions[topic]

    def __delitem__(self, topic):
        del self._subscriptions[topic]

    def get(self, topic, default=None):
        return self._subscriptions.get(topic, default)

    def iter_match(self, topic):
        for sub_topic, value in self._subscriptions.items():
            regex = sub_topic.replace('+', '[^/]+').replace('#', '.*')
            if re.fullmatch(regex, topic):
                yield value

    def iter_all_nodes(self):
        return self._subscriptions.items()


# endregion

# region MODELS
@dataclass
class XiaomiCloudDeviceInfo:
    device_id: str
    name: str
    model: str
    token: str
    spec_type: str
    local_ip: str | None
    mac: str | None
    server: str
    home_id: int
    user_id: int


# endregion

# region HTTP_CLIENT
class MIoTOauthClient:
    """oauth agent url, default: product env."""
    _main_loop: asyncio.AbstractEventLoop
    _session: aiohttp.ClientSession
    _oauth_host: str
    _client_id: int
    _redirect_url: str
    _device_id: str
    _state: str

    def __init__(
            self, client_id: str, redirect_url: str, cloud_server: str,
            uuid: str, loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        self._main_loop = loop or asyncio.get_running_loop()
        if client_id is None or client_id.strip() == '':
            raise MIoTOauthError('invalid client_id')
        if not redirect_url:
            raise MIoTOauthError('invalid redirect_url')
        if not cloud_server:
            raise MIoTOauthError('invalid cloud_server')
        if not uuid:
            raise MIoTOauthError('invalid uuid')

        self._client_id = int(client_id)
        self._redirect_url = redirect_url
        if cloud_server == 'cn':
            self._oauth_host = DEFAULT_OAUTH2_API_HOST
        else:
            self._oauth_host = f'{cloud_server}.{DEFAULT_OAUTH2_API_HOST}'
        self._device_id = f'ha.{uuid}'
        self._state = hashlib.sha1(
            f'd={self._device_id}'.encode('utf-8')).hexdigest()
        self._session = aiohttp.ClientSession(loop=self._main_loop)

    @property
    def state(self) -> str:
        return self._state

    async def deinit_async(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def set_redirect_url(self, redirect_url: str) -> None:
        if not isinstance(redirect_url, str) or redirect_url.strip() == '':
            raise MIoTOauthError('invalid redirect_url')
        self._redirect_url = redirect_url

    def gen_auth_url(
            self,
            redirect_url: Optional[str] = None,
            state: Optional[str] = None,
            scope: Optional[list] = None,
            skip_confirm: Optional[bool] = False,
    ) -> str:
        params: dict = {
            'redirect_uri': redirect_url or self._redirect_url,
            'client_id': self._client_id,
            'response_type': 'code',
            'device_id': self._device_id,
            'state': self._state
        }
        if state:
            params['state'] = state
        if scope:
            params['scope'] = ' '.join(scope).strip()
        params['skip_confirm'] = skip_confirm
        encoded_params = urlencode(params)

        return f'{OAUTH2_AUTH_URL}?{encoded_params}'

    async def __get_token_async(self, data) -> dict:
        http_res = await self._session.get(
            url=f'https://{self._oauth_host}/app/v2/ha/oauth/get_token',
            params={'data': json.dumps(data)},
            headers={'content-type': 'application/x-www-form-urlencoded'},
            timeout=MIHOME_HTTP_API_TIMEOUT
        )
        if http_res.status == 401:
            raise MIoTOauthError(
                'unauthorized(401)', MIoTErrorCode.CODE_OAUTH_UNAUTHORIZED)
        if http_res.status != 200:
            raise MIoTOauthError(
                f'invalid http status code, {http_res.status}')

        res_str = await http_res.text()
        res_obj = json.loads(res_str)
        if (
                not res_obj
                or res_obj.get('code', None) != 0
                or 'result' not in res_obj
                or not all(
            key in res_obj['result']
            for key in ['access_token', 'refresh_token', 'expires_in'])
        ):
            raise MIoTOauthError(f'invalid http response, {res_str}')

        return {
            **res_obj['result'],
            'expires_ts': int(
                time.time() +
                (res_obj['result'].get('expires_in', 0) * TOKEN_EXPIRES_TS_RATIO))
        }

    async def get_access_token_async(self, code: str) -> dict:
        if not isinstance(code, str):
            raise MIoTOauthError('invalid code')

        return await self.__get_token_async(data={
            'client_id': self._client_id,
            'redirect_uri': self._redirect_url,
            'code': code,
            'device_id': self._device_id
        })

    async def refresh_access_token_async(self, refresh_token: str) -> dict:
        if not isinstance(refresh_token, str):
            raise MIoTOauthError('invalid refresh_token')

        return await self.__get_token_async(data={
            'client_id': self._client_id,
            'redirect_uri': self._redirect_url,
            'refresh_token': refresh_token,
        })


class MIoTHttpClient:
    _main_loop: asyncio.AbstractEventLoop
    _session: aiohttp.ClientSession
    _host: str
    _base_url: str
    _client_id: str
    _access_token: str

    def __init__(
            self, session: aiohttp.ClientSession, cloud_server: str, client_id: str, access_token: str,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        self._main_loop = loop or asyncio.get_running_loop()
        self._host = DEFAULT_OAUTH2_API_HOST
        self._base_url = ''
        self._client_id = ''
        self._access_token = ''
        self._session = session

        if (
                not isinstance(cloud_server, str)
                or not isinstance(client_id, str)
                or not isinstance(access_token, str)
        ):
            raise MIoTHttpError('invalid params')

        self.update_http_header(
            cloud_server=cloud_server, client_id=client_id,
            access_token=access_token)

    async def deinit_async(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    def update_http_header(
            self, cloud_server: Optional[str] = None,
            client_id: Optional[str] = None,
            access_token: Optional[str] = None
    ) -> None:
        if isinstance(cloud_server, str):
            if cloud_server != 'cn':
                self._host = f'{cloud_server}.{DEFAULT_OAUTH2_API_HOST}'
            self._base_url = f'https://{self._host}'
        if isinstance(client_id, str):
            self._client_id = client_id
        if isinstance(access_token, str):
            self._access_token = access_token

    @property
    def __api_request_headers(self) -> dict:
        return {
            'Host': self._host,
            'X-Client-BizId': 'haapi',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer{self._access_token}',
            'X-Client-AppId': self._client_id,
        }

    async def __mihome_api_post_async(
            self, url_path: str, data: dict,
            timeout: int = MIHOME_HTTP_API_TIMEOUT
    ) -> dict:
        http_res = await self._session.post(
            url=f'{self._base_url}{url_path}',
            json=data,
            headers=self.__api_request_headers,
            timeout=timeout)
        if http_res.status == 401:
            raise MIoTHttpError(
                'mihome api get failed, unauthorized(401)',
                MIoTErrorCode.CODE_HTTP_INVALID_ACCESS_TOKEN)
        if http_res.status != 200:
            raise MIoTHttpError(
                f'mihome api post failed, {http_res.status}, '
                f'{url_path}, {data}')
        res_str = await http_res.text()
        res_obj: dict = json.loads(res_str)
        if res_obj.get('code', None) != 0:
            raise MIoTHttpError(
                f'invalid response code, {res_obj.get("code", None)}, '
                f'{res_obj.get("message", "")}')
        _LOGGER.debug(
            'mihome api post, %s%s, %s -> %s',
            self._base_url, url_path, data, res_obj)
        return res_obj

    async def get_user_info_async(self) -> dict:
        http_res = await self._session.get(
            url='https://open.account.xiaomi.com/user/profile',
            params={
                'clientId': self._client_id, 'token': self._access_token},
            headers={'content-type': 'application/x-www-form-urlencoded'},
            timeout=MIHOME_HTTP_API_TIMEOUT
        )

        res_str = await http_res.text()
        res_obj = json.loads(res_str)
        if (
                not res_obj
                or res_obj.get('code', None) != 0
                or 'data' not in res_obj
                or 'miliaoNick' not in res_obj['data']
        ):
            raise MIoTOauthError(f'invalid http response, {http_res.text}')

        return res_obj['data']

    async def get_devices_async(self, home_ids: Optional[list[str]] = None) -> dict[str, dict]:
        # This is a simplified version of the original get_devices_async
        # It fetches all devices and their home/room info
        res_obj = await self.__mihome_api_post_async(
            url_path='/app/v2/home/device_list_page',
            data={'get_split_device': True, 'get_third_device': True, 'limit': 200}
        )
        if 'result' not in res_obj:
            raise MIoTHttpError('invalid response result')

        devices = {}
        for device in res_obj['result'].get('list', []):
            did = device.get('did')
            if not did:
                continue
            devices[did] = {
                'did': did,
                'uid': device.get('uid'),
                'name': device.get('name'),
                'urn': device.get('spec_type'),
                'model': device.get('model'),
                'token': device.get('token'),
                'local_ip': device.get('localip'),
                'mac': device.get('mac'),
                'home_id': device.get('home_id'),
                'user_id': device.get('uid'),
                'server': self._host.split('.')[0] if '.' in self._host else 'cn'
            }
        return devices


# endregion

# region MQTT_CLIENT
class _MipsMsgTypeOptions(Enum):
    ID = 0
    RET_TOPIC = auto()
    PAYLOAD = auto()
    FROM = auto()
    MAX = auto()


class _MipsClient(ABC):
    MQTT_INTERVAL_S = 1
    MIPS_QOS: int = 2
    UINT32_MAX: int = 0xFFFFFFFF
    MIPS_RECONNECT_INTERVAL_MIN: float = 10
    MIPS_RECONNECT_INTERVAL_MAX: float = 600
    MIPS_SUB_PATCH: int = 300
    MIPS_SUB_INTERVAL: float = 1
    main_loop: asyncio.AbstractEventLoop
    _logger: Optional[logging.Logger]
    _client_id: str
    _host: str
    _port: int
    _username: Optional[str]
    _password: Optional[str]
    _mqtt: Optional[Client]
    _mqtt_fd: int
    _mqtt_timer: Optional[asyncio.TimerHandle]
    _mqtt_state: bool
    _event_connect: asyncio.Event
    _event_disconnect: asyncio.Event
    _internal_loop: asyncio.AbstractEventLoop
    _mips_thread: Optional[threading.Thread]
    _mips_reconnect_tag: bool
    _mips_reconnect_interval: float
    _mips_reconnect_timer: Optional[asyncio.TimerHandle]
    _mips_state_sub_map: dict[str, Any]
    _mips_state_sub_map_lock: threading.Lock
    _mips_sub_pending_map: dict[str, int]
    _mips_sub_pending_timer: Optional[asyncio.TimerHandle]

    def __init__(
            self,
            client_id: str,
            host: str,
            port: int,
            username: Optional[str] = None,
            password: Optional[str] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        self.main_loop = loop or asyncio.get_running_loop()
        self._logger = _LOGGER
        self._client_id = client_id
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._mqtt_fd = -1
        self._mqtt_timer = None
        self._mqtt_state = False
        self._mqtt = None
        self._event_connect = asyncio.Event()
        self._event_disconnect = asyncio.Event()
        self._mips_thread = None
        self._mips_reconnect_tag = False
        self._mips_reconnect_interval = 0
        self._mips_reconnect_timer = None
        self._mips_state_sub_map = {}
        self._mips_state_sub_map_lock = threading.Lock()
        self._mips_sub_pending_map = {}
        self._mips_sub_pending_timer = None

    @property
    def mips_state(self) -> bool:
        if self._mqtt:
            return self._mqtt.is_connected()
        return False

    def connect(self, thread_name: Optional[str] = None) -> None:
        if self._mips_thread:
            return
        self._internal_loop = asyncio.new_event_loop()
        self._mips_thread = threading.Thread(target=self.__mips_loop_thread)
        self._mips_thread.daemon = True
        self._mips_thread.name = (
            self._client_id if thread_name is None else thread_name)
        self._mips_thread.start()

    async def connect_async(self) -> None:
        self.connect()
        await self._event_connect.wait()

    def disconnect(self) -> None:
        if not self._mips_thread:
            return
        self._internal_loop.call_soon_threadsafe(self.__mips_disconnect)
        self._mips_thread.join()
        self._mips_thread = None
        self._internal_loop.close()

    async def disconnect_async(self) -> None:
        self.disconnect()
        await self._event_disconnect.wait()

    def __mips_loop_thread(self) -> None:
        self._mqtt = Client(client_id=self._client_id, protocol=MQTTv5)
        if self._username:
            self._mqtt.username_pw_set(
                username=self._username, password=self._password)
        self._mqtt.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self._mqtt.tls_insecure_set(True)
        self._mqtt.on_connect = self.__on_connect
        self._mqtt.on_disconnect = self.__on_disconnect
        self._mqtt.on_message = self.__on_message
        self.__mips_start_connect_tries()
        self._internal_loop.run_forever()

    def __on_connect(self, client, user_data, flags, rc, props) -> None:
        if not self._mqtt or not self._mqtt.is_connected():
            return
        self._mqtt_state = True
        self._on_mips_connect(rc, props)
        self.main_loop.call_soon_threadsafe(self._event_connect.set)

    def __on_disconnect(self, client, user_data, rc, props) -> None:
        if self._mqtt_state:
            self._mqtt_state = False
            self._on_mips_disconnect(rc, props)
        self.__mips_try_reconnect()
        self.main_loop.call_soon_threadsafe(self._event_disconnect.set)

    def __on_message(self, client: Client, user_data: Any, msg: MQTTMessage) -> None:
        self._on_mips_message(topic=msg.topic, payload=msg.payload)

    def __mips_connect(self) -> None:
        if not self._mqtt: return
        try:
            self._mqtt.connect(host=self._host, port=self._port, keepalive=MIHOME_MQTT_KEEPALIVE)
            socket = self._mqtt.socket()
            if socket is None:
                self.__mips_try_reconnect()
                return
            self._mqtt_fd = socket.fileno()
            self._internal_loop.add_reader(self._mqtt_fd, self.__mqtt_read_handler)
            self._mqtt_timer = self._internal_loop.call_later(self.MQTT_INTERVAL_S, self.__mqtt_timer_handler)
        except Exception:
            self.__mips_try_reconnect()

    def __mqtt_read_handler(self) -> None:
        if self._mqtt: self._mqtt.loop_read()

    def __mqtt_timer_handler(self) -> None:
        if self._mqtt:
            self._mqtt.loop_misc()
            self._mqtt_timer = self._internal_loop.call_later(self.MQTT_INTERVAL_S, self.__mqtt_timer_handler)

    def __mips_try_reconnect(self, immediately: bool = False) -> None:
        if self._mips_reconnect_timer: self._mips_reconnect_timer.cancel()
        if not self._mips_reconnect_tag: return
        interval = 0 if immediately else self.MIPS_RECONNECT_INTERVAL_MIN
        self._mips_reconnect_timer = self._internal_loop.call_later(interval, self.__mips_connect)

    def __mips_start_connect_tries(self) -> None:
        self._mips_reconnect_tag = True
        self.__mips_try_reconnect(immediately=True)

    def __mips_disconnect(self) -> None:
        self._mips_reconnect_tag = False
        if self._mips_reconnect_timer: self._mips_reconnect_timer.cancel()
        if self._mqtt_timer: self._mqtt_timer.cancel()
        if self._mqtt: self._mqtt.disconnect()
        self._internal_loop.stop()

    def _mips_sub_internal(self, topic: str) -> None:
        if self._mqtt and self._mqtt.is_connected():
            self._mqtt.subscribe(topic, qos=self.MIPS_QOS)

    def _mips_unsub_internal(self, topic: str) -> None:
        if self._mqtt and self._mqtt.is_connected():
            self._mqtt.unsubscribe(topic)

    @abstractmethod
    def _on_mips_message(self, topic: str, payload: bytes) -> None: ...

    @abstractmethod
    def _on_mips_connect(self, rc: int, props: dict) -> None: ...

    @abstractmethod
    def _on_mips_disconnect(self, rc: int, props: dict) -> None: ...


class MipsCloudClient(_MipsClient):
    _msg_matcher: MIoTMatcher

    def __init__(
            self, uuid: str, cloud_server: str, app_id: str,
            token: str, port: int = 8883,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ) -> None:
        self._msg_matcher = MIoTMatcher()
        host = f'{cloud_server}-ha.mqtt.io.mi.com' if cloud_server != 'cn' else 'ha.mqtt.io.mi.com'
        super().__init__(
            client_id=f'ha.{uuid}', host=host,
            port=port, username=app_id, password=token, loop=loop)

    def sub_prop(self, did: str, handler: Callable, handler_ctx: Any = None) -> bool:
        topic = f'device/{did}/up/properties_changed/#'

        def on_prop_msg(topic: str, payload: str, ctx: Any) -> None:
            try:
                msg: dict = json.loads(payload)
                if 'params' in msg:
                    handler(msg['params'], ctx)
            except json.JSONDecodeError:
                pass

        return self.__reg_broadcast_external(topic=topic, handler=on_prop_msg, handler_ctx=handler_ctx)

    def unsub_prop(self, did: str) -> bool:
        topic = f'device/{did}/up/properties_changed/#'
        return self.__unreg_broadcast_external(topic=topic)

    def __reg_broadcast_external(self, topic: str, handler: Callable, handler_ctx: Any = None) -> bool:
        self._internal_loop.call_soon_threadsafe(self.__reg_broadcast, topic, handler, handler_ctx)
        return True

    def __unreg_broadcast_external(self, topic: str) -> bool:
        self._internal_loop.call_soon_threadsafe(self.__unreg_broadcast, topic)
        return True

    def __reg_broadcast(self, topic: str, handler: Callable, handler_ctx: Any = None) -> None:
        if not self._msg_matcher.get(topic):
            self._msg_matcher[topic] = {'handler': handler, 'ctx': handler_ctx}
            self._mips_sub_internal(topic=topic)

    def __unreg_broadcast(self, topic: str) -> None:
        if self._msg_matcher.get(topic):
            del self._msg_matcher[topic]
            self._mips_unsub_internal(topic=topic)

    def _on_mips_connect(self, rc: int, props: dict) -> None:
        for topic, _ in list(self._msg_matcher.iter_all_nodes()):
            self._mips_sub_internal(topic=topic)

    def _on_mips_disconnect(self, rc: int, props: dict) -> None:
        pass

    def _on_mips_message(self, topic: str, payload: bytes) -> None:
        for item in self._msg_matcher.iter_match(topic):
            if item and item['handler']:
                payload_str = payload.decode('utf-8')
                self.main_loop.call_soon_threadsafe(item['handler'], topic, payload_str, item['ctx'])


# endregion

# region FACADE
class MiotConnector:
    _http_client: MIoTHttpClient
    _miot_client: MipsCloudClient | None
    _loop: asyncio.AbstractEventLoop
    _server: str
    _is_connected: bool

    def __init__(self, session: aiohttp.ClientSession, server: str, client_id: str, access_token: str):
        self._loop = session.loop
        self._server = server
        self._http_client = MIoTHttpClient(session, server, client_id, access_token, self._loop)
        self._miot_client = None
        self._is_connected = False

    async def connect(self):
        user_info = await self._http_client.get_user_info_async()
        user_id = user_info.get('userId')
        if not user_id:
            raise MIoTError("Failed to get user_id from token")

        self._miot_client = MipsCloudClient(
            uuid=user_id,
            cloud_server=self._server,
            app_id=MIHOME_APP_ID,
            token=self._http_client._access_token,
            loop=self._loop
        )
        await self._miot_client.connect_async()
        self._is_connected = True

    async def disconnect(self):
        if self._miot_client:
            await self._miot_client.disconnect_async()
        self._is_connected = False

    def is_authenticated(self) -> bool:
        return self._is_connected

    async def get_devices(self) -> list[XiaomiCloudDeviceInfo]:
        devices_data = await self._http_client.get_devices_async()
        devices = []
        for did, dev_info in devices_data.items():
            devices.append(XiaomiCloudDeviceInfo(
                device_id=did,
                name=dev_info.get('name'),
                model=dev_info.get('model'),
                token=dev_info.get('token'),
                spec_type=dev_info.get('urn'),
                local_ip=dev_info.get('local_ip'),
                mac=dev_info.get('mac'),
                server=dev_info.get('server'),
                home_id=dev_info.get('home_id'),
                user_id=dev_info.get('user_id')
            ))
        return devices

    async def get_device_details(self, token: str, server: Optional[str] = None) -> XiaomiCloudDeviceInfo | None:
        devices = await self.get_devices()
        for device in devices:
            if device.token == token:
                return device
        return None

    async def get_raw_map_data(self, map_url: str | None) -> bytes | None:
        if not map_url:
            return None
        async with self._http_client._session.get(map_url) as response:
            if response.status == 200:
                return await response.read()
        return None

    def sub_prop(self, did: str, handler: Callable, handler_ctx: Any = None) -> bool:
        if not self._miot_client:
            raise MIoTMipsError("MIoT client not connected.")
        return self._miot_client.sub_prop(did, handler, handler_ctx)

    def unsub_prop(self, did: str) -> bool:
        if not self._miot_client:
            raise MIoTMipsError("MIoT client not connected.")
        return self._miot_client.unsub_prop(did)

# endregion
