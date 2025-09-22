from __future__ import annotations

import logging
from typing import Any, Self, Mapping
from uuid import uuid4

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import (
    CONF_HOST, CONF_TOKEN, CONF_MAC, CONF_MODEL, CONF_DEVICE_ID, CONF_NAME, CONF_CLIENT_ID
)
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_create_clientsession
from homeassistant.helpers.device_registry import format_mac
from homeassistant.helpers.network import get_url
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)
from miio import RoborockVacuum
from vacuum_map_parser_base.config.color import ColorsPalette
from vacuum_map_parser_base.config.drawable import Drawable
from vacuum_map_parser_base.config.image_config import ImageConfig
from vacuum_map_parser_base.config.size import Sizes

from .connector.vacuums.base.model import VacuumApi
from .connector.xiaomi_cloud.miot_connector import MiotConnector, XiaomiCloudDeviceInfo, MIoTOauthClient, MIHOME_APP_ID
from .connector.xiaomi_cloud.const import AVAILABLE_SERVERS
from .const import (
    DOMAIN,
    CONF_USED_MAP_API,
    CONF_SERVER,
    CONF_COLORS,
    CONF_IMAGE_CONFIG,
    CONF_ROOM_COLORS,
    CONF_DRAWABLES,
    CONF_SIZES,
    CONF_TEXTS,
    CONF_IMAGE_CONFIG_SCALE,
    CONF_IMAGE_CONFIG_ROTATE,
    CONF_IMAGE_CONFIG_TRIM_LEFT,
    CONF_IMAGE_CONFIG_TRIM_BOTTOM,
    CONF_IMAGE_CONFIG_TRIM_TOP,
    CONF_IMAGE_CONFIG_TRIM_RIGHT,
    CONF_TOKEN_DATA
)
from .options_flow import XiaomiCloudMapExtractorOptionsFlowHandler
from .types import XiaomiCloudMapExtractorConfigEntry

_LOGGER = logging.getLogger(__name__)


class XiaomiCloudMapExtractorFlowHandler(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self) -> None:
        """Initialize."""
        self.server: str | None = None
        self.token_data: dict[str, Any] | None = None
        self.cloud_vacuums: list[XiaomiCloudDeviceInfo] = []
        self.cloud_vacuum: XiaomiCloudDeviceInfo | None = None
        self.oauth_client: MIoTOauthClient | None = None

    @staticmethod
    @callback
    def async_get_options_flow(
            config_entry: XiaomiCloudMapExtractorConfigEntry) -> XiaomiCloudMapExtractorOptionsFlowHandler:
        """Get the options flow."""
        return XiaomiCloudMapExtractorOptionsFlowHandler()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle a flow initialized by the user."""
        if user_input is not None:
            self.server = user_input[CONF_SERVER]
            return await self.async_step_auth()

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_SERVER, default="de"): vol.In(AVAILABLE_SERVERS)
            })
        )

    async def async_step_auth(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Redirect user to Xiaomi to authorize."""
        redirect_uri = f"{get_url(self.hass, require_current_request=True)}/auth/external/callback"
        self.oauth_client = MIoTOauthClient(
            client_id=MIHOME_APP_ID,
            redirect_url=redirect_uri,
            cloud_server=self.server,
            uuid=str(uuid4())
        )
        auth_url = self.oauth_client.gen_auth_url()

        return self.async_external_step(step_id="auth_callback", url=auth_url)

    async def async_step_auth_callback(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle authentication callback from external auth."""
        if not user_input or "code" not in user_input:
            return self.async_abort(reason="auth_error")

        code = user_input["code"]

        if not self.oauth_client:
            return self.async_abort(reason="missing_server")

        try:
            self.token_data = await self.oauth_client.get_access_token_async(code)
        except Exception as e:
            _LOGGER.error("Failed to get access token: %s", e, exc_info=True)
            return self.async_abort(reason="auth_error")

        session = async_create_clientsession(self.hass)
        connector = MiotConnector(session, self.server, MIHOME_APP_ID, self.token_data["access_token"])

        try:
            await connector.connect()
            devices_raw = await connector.get_devices()
            self.cloud_vacuums = [device for device in devices_raw if "vacuum" in device.spec_type]

            if not self.cloud_vacuums:
                return self.async_abort(reason="no_devices")

            if len(self.cloud_vacuums) == 1:
                self.cloud_vacuum = self.cloud_vacuums[0]
                return await self.async_step_confirm_data()

            return await self.async_step_select_vacuum()

        except Exception as e:
            _LOGGER.error("Failed to connect to Xiaomi Cloud or get devices: %s", e, exc_info=True)
            return self.async_abort(reason="auth_error")

    async def async_step_select_vacuum(
            self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle multiple cloud devices found."""
        errors: dict[str, str] = {}
        if user_input is not None:
            self.cloud_vacuum = next(filter(lambda v: v.device_id == user_input["select_vacuum"], self.cloud_vacuums))
            return await self.async_step_confirm_data()

        options: list[SelectOptionDict] = [
            SelectOptionDict(value=cloud_vacuum.device_id,
                             label=f"{cloud_vacuum.name} - {cloud_vacuum.model} ({cloud_vacuum.mac})")
            for cloud_vacuum in self.cloud_vacuums
        ]

        select_schema = vol.Schema(
            {vol.Required("select_vacuum"): SelectSelector(
                SelectSelectorConfig(
                    options=options,
                    custom_value=False,
                    sort=True,
                    mode=SelectSelectorMode.LIST,
                ))}
        )

        return self.async_show_form(
            step_id="select_vacuum", data_schema=select_schema, errors=errors
        )

    async def async_step_confirm_data(
            self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            host = user_input.get(CONF_HOST)
            token = user_input.get(CONF_TOKEN)
            used_map_api = user_input.get(CONF_USED_MAP_API)

            if not await self._validate_vacuum(host, token, VacuumApi(used_map_api)):
                errors["base"] = "invalid_vacuum"
            else:
                unique_id = format_mac(self.cloud_vacuum.mac)
                await self.async_set_unique_id(unique_id, raise_on_progress=False)
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=self.cloud_vacuum.name,
                    data={
                        CONF_HOST: host,
                        CONF_TOKEN: token,
                        CONF_DEVICE_ID: self.cloud_vacuum.device_id,
                        CONF_MODEL: self.cloud_vacuum.model,
                        CONF_MAC: format_mac(self.cloud_vacuum.mac),
                        CONF_NAME: self.cloud_vacuum.name,
                        CONF_SERVER: self.server,
                        CONF_TOKEN_DATA: self.token_data,
                        CONF_CLIENT_ID: MIHOME_APP_ID,
                        CONF_USED_MAP_API: used_map_api,
                    },
                    options={
                        CONF_IMAGE_CONFIG: self._default_image_config(),
                        CONF_COLORS: self._default_colors(),
                        CONF_ROOM_COLORS: {},
                        CONF_DRAWABLES: [
                            e.value for e in Drawable if
                            e != Drawable.ROOM_NAMES and "ignored" not in e
                        ],
                        CONF_SIZES: {k.value: v for k, v in Sizes.SIZES.items()},
                        CONF_TEXTS: [],
                    }
                )

        detected_api = VacuumApi.detect(self.cloud_vacuum.model)
        api_options: list[SelectOptionDict] = [
            SelectOptionDict(value=v, label=v.title() + (" *" if v == detected_api else "")) for v in VacuumApi
        ]
        confirm_data_schema = vol.Schema({
            vol.Required(CONF_HOST, default=self.cloud_vacuum.local_ip): str,
            vol.Required(CONF_TOKEN, default=self.cloud_vacuum.token): vol.All(str, vol.Length(min=32, max=32)),
            vol.Required(CONF_USED_MAP_API, default=detected_api): SelectSelector(
                SelectSelectorConfig(
                    options=api_options,
                    custom_value=False,
                    sort=False,
                    mode=SelectSelectorMode.LIST,
                ))
        })

        return self.async_show_form(
            step_id="confirm_data", data_schema=confirm_data_schema, errors=errors, last_step=True
        )

    async def _validate_vacuum(self: Self, host: str, token: str, used_map_api: VacuumApi) -> bool:
        if used_map_api != VacuumApi.ROBOROCK:
            return True
        roborock_vacuum = RoborockVacuum(host, token)
        try:
            status = await self.hass.async_add_executor_job(roborock_vacuum.status)
            return status is not None
        except Exception as e:
            _LOGGER.error(e, exc_info=True)
            return False

    def _default_image_config(self: Self) -> dict[str, float]:
        image_config = ImageConfig()
        return {
            CONF_IMAGE_CONFIG_SCALE: image_config.scale,
            CONF_IMAGE_CONFIG_ROTATE: image_config.rotate,
            CONF_IMAGE_CONFIG_TRIM_LEFT: image_config.trim.left,
            CONF_IMAGE_CONFIG_TRIM_RIGHT: image_config.trim.right,
            CONF_IMAGE_CONFIG_TRIM_TOP: image_config.trim.top,
            CONF_IMAGE_CONFIG_TRIM_BOTTOM: image_config.trim.bottom,
        }

    @staticmethod
    def _default_colors() -> dict[str, tuple[int, int, int, int]]:
        return {k: ([*v] if len(v) == 4 else [*v, 255]) for k, v in ColorsPalette.COLORS.items()}
