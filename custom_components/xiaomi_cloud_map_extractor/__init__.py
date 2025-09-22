from __future__ import annotations

import logging

from homeassistant.const import (
    CONF_HOST,
    CONF_TOKEN,
    CONF_MAC,
    CONF_MODEL,
    CONF_DEVICE_ID,
    CONF_NAME,
    CONF_CLIENT_ID
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from vacuum_map_parser_base.config.color import ColorsPalette
from vacuum_map_parser_base.config.drawable import Drawable
from vacuum_map_parser_base.config.image_config import ImageConfig, TrimConfig
from vacuum_map_parser_base.config.size import Sizes

from .connector import XiaomiCloudMapExtractorConnector
from .connector.model import XiaomiCloudMapExtractorConnectorConfiguration
from .connector.vacuums.base.model import VacuumApi
from .connector.xiaomi_cloud.miot_connector import MiotConnector
from .const import (
    DOMAIN,
    PLATFORMS,
    CONF_SERVER,
    CONF_USED_MAP_API,
    CONF_TOKEN_DATA,
    CONF_IMAGE_CONFIG,
    CONF_COLORS,
    CONF_ROOM_COLORS,
    CONF_DRAWABLES,
    CONF_SIZES,
    CONF_IMAGE_CONFIG_SCALE,
    CONF_IMAGE_CONFIG_ROTATE,
    CONF_IMAGE_CONFIG_TRIM_LEFT,
    CONF_IMAGE_CONFIG_TRIM_RIGHT,
    CONF_IMAGE_CONFIG_TRIM_TOP,
    CONF_IMAGE_CONFIG_TRIM_BOTTOM
)
from .coordinator import XiaomiCloudMapExtractorDataUpdateCoordinator
from .types import XiaomiCloudMapExtractorConfigEntry, XiaomiCloudMapExtractorRuntimeData

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: XiaomiCloudMapExtractorConfigEntry) -> bool:
    """Set up Xiaomi Cloud Map Extractor from a config entry."""
    config = to_configuration(entry)
    session = async_get_clientsession(hass)
    
    token_data = entry.data[CONF_TOKEN_DATA]
    access_token = token_data["access_token"]
    client_id = entry.data[CONF_CLIENT_ID]
    server = entry.data[CONF_SERVER]

    miot_connector = MiotConnector(session, server, client_id, access_token)
    connector = XiaomiCloudMapExtractorConnector(miot_connector, config)
    coordinator = XiaomiCloudMapExtractorDataUpdateCoordinator(hass, connector)

    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = XiaomiCloudMapExtractorRuntimeData(coordinator)

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))

    return True


async def async_unload_entry(hass: HomeAssistant, entry: XiaomiCloudMapExtractorConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok


async def async_reload_entry(hass: HomeAssistant, entry: XiaomiCloudMapExtractorConfigEntry) -> None:
    """Reload config entry."""
    await hass.config_entries.async_reload(entry.entry_id)


def to_configuration(entry: XiaomiCloudMapExtractorConfigEntry) -> XiaomiCloudMapExtractorConnectorConfiguration:
    """Convert config entry to a configuration object."""
    # Data from the final step of the config flow
    host = entry.data[CONF_HOST]
    token = entry.data[CONF_TOKEN]
    device_id = entry.data[CONF_DEVICE_ID]
    model = entry.data[CONF_MODEL]
    mac = entry.data[CONF_MAC]
    server = entry.data[CONF_SERVER]
    used_api = VacuumApi(entry.data[CONF_USED_MAP_API])

    # Options from the options flow (or defaults)
    # As we don't have an options flow yet, we use empty defaults
    options = entry.options or {}
    image_config_data = options.get(CONF_IMAGE_CONFIG, {})
    scale = image_config_data.get(CONF_IMAGE_CONFIG_SCALE, 4)
    rotate = image_config_data.get(CONF_IMAGE_CONFIG_ROTATE, 0)
    trim_left = image_config_data.get(CONF_IMAGE_CONFIG_TRIM_LEFT, 0)
    trim_right = image_config_data.get(CONF_IMAGE_CONFIG_TRIM_RIGHT, 0)
    trim_top = image_config_data.get(CONF_IMAGE_CONFIG_TRIM_TOP, 0)
    trim_bottom = image_config_data.get(CONF_IMAGE_CONFIG_TRIM_BOTTOM, 0)
    image_config = ImageConfig(scale, rotate, TrimConfig(trim_left, trim_right, trim_top, trim_bottom))

    colors = ColorsPalette(options.get(CONF_COLORS, {}), options.get(CONF_ROOM_COLORS, {}))
    drawables = [Drawable(e) for e in options.get(CONF_DRAWABLES, [])]
    sizes = Sizes(options.get(CONF_SIZES, {}))
    texts = options.get(CONF_TEXTS, [])

    return XiaomiCloudMapExtractorConnectorConfiguration(
        host=host,
        token=token,
        username="",  # Not used in OAuth flow
        password="",  # Not used in OAuth flow
        server=server,
        used_api=used_api,
        device_id=device_id,
        mac=mac,
        model=model,
        image_config=image_config,
        colors=colors,
        drawables=drawables,
        sizes=sizes,
        texts=texts,
        store_map_raw=False,
        store_map_image=False,
        store_map_path=""
    )
