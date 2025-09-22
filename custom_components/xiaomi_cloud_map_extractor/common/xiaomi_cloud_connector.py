import base64
import hashlib
import hmac
import json
import logging
import os
import random
import tempfile
import time
from typing import Any, Dict, NamedTuple, Optional, Tuple

import requests
from Cryptodome.Cipher import ARC4
from PIL import Image

from custom_components.xiaomi_cloud_map_extractor.const import *

_LOGGER = logging.getLogger(__name__)


class XiaomiHome(NamedTuple):
    homeid: int
    owner: int


class XiaomiDeviceInfo(NamedTuple):
    device_id: str
    name: str
    model: str
    token: str
    country: str
    home_id: int
    user_id: int


# noinspection PyBroadException
class XiaomiCloudConnector:

    def __init__(self, username: str, password: str):
        self.two_factor_auth_url = None
        self.captcha_url = None
        self._username = username
        self._password = password
        self._agent = self.generate_agent()
        self._device_id = self.generate_device_id()
        self._session = requests.session()
        self._sign = None
        self._ssecurity = None
        self.userId = None
        self._cUserId = None
        self._passToken = None
        self._location = None
        self._code = None
        self._serviceToken = None

    def login_step_1(self):
        _LOGGER.debug("login_step_1")
        url = "https://account.xiaomi.com/pass/serviceLogin?sid=xiaomiio&_json=true"
        headers = {
            "User-Agent": self._agent,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        cookies = {
            "userId": self._username
        }
        try:
            response = self._session.get(url, headers=headers, cookies=cookies, timeout=10)
            _LOGGER.debug(response.text)
            json_resp = self.to_json(response.text)
            if response.status_code == 200:
                if "_sign" in json_resp:
                    self._sign = json_resp["_sign"]
                    return True
                elif "ssecurity" in json_resp:
                    self._ssecurity = json_resp["ssecurity"]
                    self.userId = json_resp["userId"]
                    self._cUserId = json_resp["cUserId"]
                    self._passToken = json_resp["passToken"]
                    self._location = json_resp["location"]
                    self._code = json_resp["code"]
                    return True
        except Exception as e:
            _LOGGER.error("Error in login_step_1: %s", e)
        return False

    def login_step_2(self) -> Optional[bool]:
        _LOGGER.debug("login_step_2")
        url: str = "https://account.xiaomi.com/pass/serviceLoginAuth2"
        headers: dict = {
            "User-Agent": self._agent,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        fields: dict = {
            "sid": "xiaomiio",
            "hash": hashlib.md5(str.encode(self._password)).hexdigest().upper(),
            "callback": "https://sts.api.io.mi.com/sts",
            "qs": "%3Fsid%3Dxiaomiio%26_json%3Dtrue",
            "user": self._username,
            "_sign": self._sign,
            "_json": "true"
        }
        try:
            response = self._session.post(url, headers=headers, params=fields, allow_redirects=False, timeout=10)
            _LOGGER.debug("login_step_2: Response text: %s", response.text)
            if response.status_code == 200:
                json_resp: dict = self.to_json(response.text)

                if "captchaUrl" in json_resp and json_resp["captchaUrl"] is not None:
                    self.captcha_url = json_resp["captchaUrl"]
                    _LOGGER.error("Captcha required. Restart integration to try again. Captcha URL: %s", self.captcha_url)
                    self.handle_captcha(self.captcha_url)
                    return False

                if "ssecurity" in json_resp and len(str(json_resp["ssecurity"])) > 4:
                    self._ssecurity = json_resp["ssecurity"]
                    self.userId = json_resp.get("userId", None)
                    self._cUserId = json_resp.get("cUserId", None)
                    self._passToken = json_resp.get("passToken", None)
                    self._location = json_resp.get("location", None)
                    self._code = json_resp.get("code", None)
                    self.two_factor_auth_url = None
                    return True
                elif "notificationUrl" in json_resp:
                    _LOGGER.error(
                        "Additional authentication required. Open following URL using device that has the same public IP "
                        "as your Home Assistant instance: %s",
                        json_resp["notificationUrl"])
                    self.two_factor_auth_url = json_resp["notificationUrl"]
                    return None  # Signal 2FA requirement
                else:
                    _LOGGER.error("login_step_2: Login failed, server returned: %s", json_resp)
            else:
                _LOGGER.error("login_step_2: HTTP status: %s; Response: %s", response.status_code, response.text[:500])
        except Exception as e:
            _LOGGER.error("Error in login_step_2: %s", e)
        return False

    def verify_ticket(self, verify_url, ticket):
        path = 'identity/authStart'
        if path not in verify_url:
            return None
        resp = self._session.get(verify_url.replace(path, 'identity/list'))
        identity_session = resp.cookies.get('identity_session')
        if not identity_session:
            return False
        data = self.to_json(resp.text) or {}
        flag = data.get('flag', 4)
        options = data.get('options', [flag])

        for flag in options:
            api = {
                4: '/identity/auth/verifyPhone',
                8: '/identity/auth/verifyEmail',
            }.get(flag)
            if not api:
                continue
            resp = self._session.post(
                'https://account.xiaomi.com' + api,
                params={
                    '_dc': int(time.time() * 1000),
                },
                data={
                    '_flag': flag,
                    'ticket': ticket,
                    'trust': 'true',
                    '_json': 'true',
                },
                cookies={
                    'identity_session': identity_session,
                },
            )
            data = self.to_json(resp.text)
            if data.get('code') == 0:
                return data
        return False

    def login_step_3(self) -> bool:
        _LOGGER.debug("login_step_3")
        headers = {
            "User-Agent": self._agent,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        try:
            response = self._session.get(self._location, headers=headers, timeout=10)
            _LOGGER.debug(response.text)
            if response.status_code == 200 and "serviceToken" in response.cookies:
                self._serviceToken = response.cookies.get("serviceToken")
                return True
        except Exception as e:
            _LOGGER.error("Error in login_step_3: %s", e)
        return False

    def login(self) -> Optional[bool]:
        self._session.close()
        self._session = requests.session()
        self._agent = self.generate_agent()
        self._device_id = self.generate_device_id()
        self._session.cookies.set("sdkVersion", "accountsdk-18.8.15", domain="mi.com")
        self._session.cookies.set("sdkVersion", "accountsdk-18.8.15", domain="xiaomi.com")
        self._session.cookies.set("deviceId", self._device_id, domain="mi.com")
        self._session.cookies.set("deviceId", self._device_id, domain="xiaomi.com")

        if self.login_step_1():
            if self._ssecurity is None:  # Got _sign, need to do step 2
                login_step_2_result = self.login_step_2()
                if login_step_2_result is None:  # 2FA
                    return None
                if not login_step_2_result:  # Failed (e.g. captcha, bad password)
                    return False
            # if we are here, we have ssecurity, either from step 1 or 2
            if self.login_step_3():
                return True

        return False

    def handle_captcha(self, captcha_url: str):
        if captcha_url.startswith("/"):
            captcha_url = "https://account.xiaomi.com" + captcha_url
        _LOGGER.debug("Downloading captcha image from: %s", captcha_url)
        try:
            response = self._session.get(captcha_url, stream=False, timeout=10)
            if response.status_code == 200:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
                    tmp.write(response.content)
                    tmp_path: str = tmp.name
                _LOGGER.warning(f"Captcha image saved at: {tmp_path}")
                try:
                    img = Image.open(tmp_path)
                    img.show()
                except Exception as e:
                    _LOGGER.debug("Failed to show captcha image: %s", e)
                    _LOGGER.warning(f"Please open {tmp_path} and solve the captcha.")
            else:
                _LOGGER.error("Unable to fetch captcha image.")
        except Exception as e:
            _LOGGER.error("Failed to handle captcha: %s", e)

    def get_raw_map_data(self, map_url) -> Optional[bytes]:
        if map_url is not None:
            try:
                response = self._session.get(map_url, timeout=10)
            except:
                response = None
            if response is not None and response.status_code == 200:
                return response.content
        return None

    def get_homes_iter(self, country: str):
        url = self.get_api_url(country) + "/v2/homeroom/gethome"
        params = {
            "data": json.dumps(
                {
                    "fg": True,
                    "fetch_share": True,
                    "fetch_share_dev": True,
                    "limit": 300,
                    "app_ver": 7,
                }
            )
        }
        response = self.execute_api_call_encrypted(url, params)
        if response is None:
            return None
        if "result" in response and "homelist" in response["result"]:
            homelist = response["result"]["homelist"]
            yield from (XiaomiHome(int(home["id"]), home["uid"]) for home in homelist)
        if "result" in response and "share_home_list" in response["result"]:
            share_home_list = response["result"]["share_home_list"]
            yield from (XiaomiHome(int(home["id"]), home["uid"]) for home in share_home_list)

    def get_devices_from_home_iter(self, country: str, home_id: int, owner_id: int):
        url = self.get_api_url(country) + "/v2/home/home_device_list"
        params = {
            "data": json.dumps(
                {
                    "home_id": home_id,
                    "home_owner": owner_id,
                    "limit": 200,
                    "get_split_device": True,
                    "support_smart_home": True,
                }
            )
        }
        response = self.execute_api_call_encrypted(url, params)
        if response is None:
            return
        if "result" in response and "device_info" in response["result"]:
            raw_devices = response["result"]["device_info"]
            if raw_devices is None:
                return
            yield from (
                XiaomiDeviceInfo(
                    device_id=device["did"],
                    name=device["name"],
                    model=device["model"],
                    token=device["token"],
                    country=country,
                    user_id=owner_id,
                    home_id=home_id,
                )
                for device in raw_devices
            )

    def get_devices_iter(self, country: Optional[str] = None):
        countries_to_check = CONF_AVAILABLE_COUNTRIES if country is None else [country]
        for _country in countries_to_check:
            homes = self.get_homes_iter(_country)
            if homes is not None:
                for home in homes:
                    devices = self.get_devices_from_home_iter(
                        _country, home.homeid, home.owner
                    )
                    yield from devices

    def get_device_details(self, token: str,
                           country: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        devices = self.get_devices_iter(country)
        if devices is not None:
            matching_token = filter(lambda device: device.token == token, devices)
            if match := next(matching_token, None):
                return match.country, str(match.user_id), match.device_id, match.model
        return None, None, None, None

    def execute_api_call_encrypted(self, url: str, params: Dict[str, str]) -> Any:
        headers = {
            "Accept-Encoding": "identity",
            "User-Agent": self._agent,
            "Content-Type": "application/x-www-form-urlencoded",
            "x-xiaomi-protocal-flag-cli": "PROTOCAL-HTTP2",
            "MIOT-ENCRYPT-ALGORITHM": "ENCRYPT-RC4",
        }
        cookies = {
            "userId": str(self.userId),
            "yetAnotherServiceToken": str(self._serviceToken),
            "serviceToken": str(self._serviceToken),
            "locale": "en_GB",
            "timezone": "GMT+02:00",
            "is_daylight": "1",
            "dst_offset": "3600000",
            "channel": "MI_APP_STORE"
        }
        millis = round(time.time() * 1000)
        nonce = self.generate_nonce(millis)
        signed_nonce = self.signed_nonce(nonce)
        fields = self.generate_enc_params(url, "POST", signed_nonce, nonce, params, self._ssecurity)

        try:
            response = self._session.post(url, headers=headers, cookies=cookies, params=fields, timeout=10)
            if response.status_code == 200:
                decoded = self.decrypt_rc4(self.signed_nonce(fields["_nonce"]), response.text)
                return json.loads(decoded)
        except Exception as e:
            _LOGGER.error("Error executing api call: %s", e)
        return None

    @staticmethod
    def get_api_url(country: str) -> str:
        return "https://" + ("" if country == "cn" else (country + ".")) + "api.io.mi.com/app"

    def signed_nonce(self, nonce: str) -> str:
        hash_object = hashlib.sha256(base64.b64decode(self._ssecurity) + base64.b64decode(nonce))
        return base64.b64encode(hash_object.digest()).decode('utf-8')

    @staticmethod
    def generate_nonce(millis: int):
        nonce_bytes = os.urandom(8) + (int(millis / 60000)).to_bytes(4, byteorder='big')
        return base64.b64encode(nonce_bytes).decode()

    @staticmethod
    def generate_agent() -> str:
        agent_id = "".join(
            map(lambda i: chr(i), [random.randint(65, 69) for _ in range(13)])
        )
        random_text = "".join(map(lambda i: chr(i), [random.randint(97, 122) for _ in range(18)]))
        return f"{random_text}-{agent_id} APP/com.xiaomi.mihome APPV/10.5.201"

    @staticmethod
    def generate_device_id() -> str:
        return "".join((chr(random.randint(97, 122)) for _ in range(6)))

    @staticmethod
    def generate_enc_signature(url, method: str, signed_nonce: str, params: Dict[str, str]) -> str:
        signature_params = [str(method).upper(), url.split("com")[1].replace("/app/", "/")]
        for k, v in params.items():
            signature_params.append(f"{k}={v}")
        signature_params.append(signed_nonce)
        signature_string = "&".join(signature_params)
        return base64.b64encode(hashlib.sha1(signature_string.encode('utf-8')).digest()).decode()

    @staticmethod
    def generate_enc_params(url: str, method: str, signed_nonce: str, nonce: str, params: Dict[str, str],
                            ssecurity: str) -> Dict[str, str]:
        params['rc4_hash__'] = XiaomiCloudConnector.generate_enc_signature(url, method, signed_nonce, params)
        for k, v in params.items():
            params[k] = XiaomiCloudConnector.encrypt_rc4(signed_nonce, v)
        params.update({
            'signature': XiaomiCloudConnector.generate_enc_signature(url, method, signed_nonce, params),
            'ssecurity': ssecurity,
            '_nonce': nonce,
        })
        return params

    @staticmethod
    def to_json(response_text: str) -> Any:
        return json.loads(response_text.replace("&&&START&&&", ""))

    @staticmethod
    def encrypt_rc4(password: str, payload: str) -> str:
        r = ARC4.new(base64.b64decode(password))
        r.encrypt(bytes(1024))
        return base64.b64encode(r.encrypt(payload.encode())).decode()

    @staticmethod
    def decrypt_rc4(password: str, payload: str) -> bytes:
        r = ARC4.new(base64.b64decode(password))
        r.encrypt(bytes(1024))
        return r.encrypt(base64.b64decode(payload))
