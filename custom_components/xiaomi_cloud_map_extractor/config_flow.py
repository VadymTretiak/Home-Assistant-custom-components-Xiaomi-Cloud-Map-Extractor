from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import (
    CONF_HOST, CONF_TOKEN, CONF_MAC, CONF_MODEL, CONF_DEVICE_ID, CONF_NAME, CONF_CLIENT_ID
)
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import format_mac
from homeassistant.helpers.network import get_url
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .connector.vacuums.base.model import VacuumApi
from .connector.xiaomi_cloud.const import AVAILABLE_SERVERS
from .connector.xiaomi_cloud.miot_connector import MiotConnector, XiaomiCloudDeviceInfo, MIoTOauthClient, MIHOME_APP_ID
from .const import (
    DOMAIN,
    CONF_USED_MAP_API,
    CONF_SERVER,
    CONF_TOKEN_DATA
)
from .types import XiaomiCloudMapExtractorConfigEntry

_LOGGER = logging.getLogger(__name__)


class XiaomiCloudMapExtractorFlowHandler(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Xiaomi Cloud Map Extractor."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self.server: str | None = None
        self.oauth_client: MIoTOauthClient | None = None
        self.token_data: dict[str, Any] | None = None
        self.cloud_vacuums: list[XiaomiCloudDeviceInfo] = []
        self.cloud_vacuum: XiaomiCloudDeviceInfo | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: XiaomiCloudMapExtractorConfigEntry) -> OptionsFlow:
        return XiaomiCloudMapExtractorOptionsFlowHandler(config_entry)

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle the initial step."""
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
        """Handle the callback from Xiaomi."""
        if not user_input or "code" not in user_input:
            return self.async_abort(reason="auth_error_no_code")

        if not self.oauth_client:
            return self.async_abort(reason="auth_error_no_client")

        try:
            self.token_data = await self.oauth_client.get_access_token_async(user_input["code"])
        except Exception as e:
            _LOGGER.error("Failed to get access token: %s", e, exc_info=True)
            return self.async_abort(reason="auth_error_token")

        return await self.async_step_device_selection()

    async def async_step_device_selection(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Fetch devices and allow user to select one."""
        session = async_get_clientsession(self.hass)
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
            return self.async_abort(reason="auth_error_device_fetch")

    async def async_step_select_vacuum(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Handle multiple cloud devices found."""
        if user_input is not None:
            self.cloud_vacuum = next(filter(lambda v: v.device_id == user_input["select_vacuum"], self.cloud_vacuums))
            return await self.async_step_confirm_data()

        options = [
            SelectOptionDict(value=v.device_id, label=f"{v.name} - {v.model} ({v.mac})")
            for v in self.cloud_vacuums
        ]

        return self.async_show_form(
            step_id="select_vacuum",
            data_schema=vol.Schema({
                vol.Required("select_vacuum"): SelectSelector(SelectSelectorConfig(options=options, mode=SelectSelectorMode.LIST))
            })
        )

    async def async_step_confirm_data(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Final step to confirm local device details and create the entry."""
        if user_input is not None:
            unique_id = format_mac(self.cloud_vacuum.mac)
            await self.async_set_unique_id(unique_id)
            self._abort_if_unique_id_configured()

            # We are creating the main config entry here, not in an options flow
            return self.async_create_entry(
                title=self.cloud_vacuum.name,
                data={
                    CONF_HOST: user_input[CONF_HOST],
                    CONF_TOKEN: user_input[CONF_TOKEN],
                    CONF_DEVICE_ID: self.cloud_vacuum.device_id,
                    CONF_MODEL: self.cloud_vacuum.model,
                    CONF_MAC: unique_id,
                    CONF_NAME: self.cloud_vacuum.name,
                    CONF_SERVER: self.server,
                    CONF_TOKEN_DATA: self.token_data,
                    CONF_CLIENT_ID: MIHOME_APP_ID,
                    CONF_USED_MAP_API: user_input[CONF_USED_MAP_API],
                }
            )

        detected_api = VacuumApi.detect(self.cloud_vacuum.model)
        return self.async_show_form(
            step_id="confirm_data",
            data_schema=vol.Schema({
                vol.Required(CONF_HOST, default=self.cloud_vacuum.local_ip or ""):
                    str,
                vol.Required(CONF_TOKEN, default=self.cloud_vacuum.token):
                    vol.All(str, vol.Length(min=32, max=32)),
                vol.Required(CONF_USED_MAP_API, default=detected_api.value):
                    SelectSelector(SelectSelectorConfig(options=[SelectOptionDict(value=v.value, label=v.title()) for v in VacuumApi], mode=SelectSelectorMode.LIST))
            }),
            last_step=True
        )


class XiaomiCloudMapExtractorOptionsFlowHandler(OptionsFlow):
    """This options flow is now a placeholder as setup is handled in the main flow."""

    def __init__(self, config_entry: XiaomiCloudMapExtractorConfigEntry):
        self.config_entry = config_entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Manage the options."""
        # Currently, there are no options to configure after setup.
        # This can be expanded in the future.
        return self.async_create_entry(title="", data={})
