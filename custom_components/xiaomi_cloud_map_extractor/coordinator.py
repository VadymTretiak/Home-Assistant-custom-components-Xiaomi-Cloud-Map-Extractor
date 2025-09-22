import logging
from typing import Self

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .connector import XiaomiCloudMapExtractorConnector
from .connector.model import XiaomiCloudMapExtractorData
from .connector.utils.exceptions import XiaomiCloudMapExtractorException
from .connector.xiaomi_cloud.miot_connector import MIoTHttpError, MIoTErrorCode
from .const import DOMAIN, DEFAULT_UPDATE_INTERVAL

_LOGGER = logging.getLogger(__name__)


class XiaomiCloudMapExtractorDataUpdateCoordinator(DataUpdateCoordinator[XiaomiCloudMapExtractorData]):

    def __init__(
            self: Self,
            hass: HomeAssistant,
            connector: XiaomiCloudMapExtractorConnector,
    ) -> None:
        self.connector = connector
        super().__init__(hass, _LOGGER, name=DOMAIN, update_interval=DEFAULT_UPDATE_INTERVAL,
                         update_method=self.update_data)

    async def update_data(self: Self) -> XiaomiCloudMapExtractorData:
        try:
            return await self.connector.get_data()
        except MIoTHttpError as err:
            if err.code == MIoTErrorCode.CODE_HTTP_INVALID_ACCESS_TOKEN:
                _LOGGER.error("Access token expired or invalid, triggering reauth")
                raise ConfigEntryAuthFailed(err) from err
            _LOGGER.error("HTTP error while updating data: %s", err)
            raise UpdateFailed(err) from err
        except XiaomiCloudMapExtractorException as err:
            _LOGGER.error("Failed to update data: %s", err)
            raise UpdateFailed(err) from err
