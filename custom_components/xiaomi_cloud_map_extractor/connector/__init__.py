import logging
from typing import Type

from .model import XiaomiCloudMapExtractorData, XiaomiCloudMapExtractorConnectorConfiguration
from .utils import to_image
from .utils.exceptions import DeviceNotFoundException, FailedMapDownloadException
from .vacuums.base.model import VacuumConfig, VacuumApi
from .vacuums.base.vacuum_base import BaseXiaomiCloudVacuum
from .vacuums.vacuum_dreame import DreameCloudVacuum
from .vacuums.vacuum_roborock import RoborockCloudVacuum
from .vacuums.vacuum_roidmi import RoidmiCloudVacuum
from .vacuums.vacuum_unsupported import UnsupportedCloudVacuum
from .vacuums.vacuum_viomi import ViomiCloudVacuum
from .xiaomi_cloud.miot_connector import MiotConnector

_LOGGER = logging.getLogger(__name__)

AVAILABLE_VACUUM_PLATFORMS: dict[VacuumApi, Type[BaseXiaomiCloudVacuum]] = {
    v.vacuum_platform(): v for v in [
        RoborockCloudVacuum,
        ViomiCloudVacuum,
        RoidmiCloudVacuum,
        DreameCloudVacuum,
        UnsupportedCloudVacuum
    ]
}


class XiaomiCloudMapExtractorConnector:
    """Orchestrates the retrieval of map data from Xiaomi Cloud."""

    def __init__(self, miot_connector: MiotConnector, config: XiaomiCloudMapExtractorConnectorConfiguration):
        self._miot_connector = miot_connector
        self._config = config
        self._vacuum_connector: BaseXiaomiCloudVacuum | None = None

    async def get_data(self) -> XiaomiCloudMapExtractorData:
        """Retrieve and parse the map data."""
        if self._vacuum_connector is None:
            await self._initialize()

        map_data, map_saved, map_raw_data = await self._vacuum_connector.get_map()
        if map_data is None:
            raise FailedMapDownloadException("Failed to download map data")

        extractor_data = XiaomiCloudMapExtractorData()
        extractor_data.map_data = map_data
        extractor_data.map_saved = map_saved
        extractor_data.map_image = to_image(map_data)
        extractor_data.map_data_raw = map_raw_data
        return extractor_data

    async def _initialize(self):
        """Initialize the vacuum-specific connector."""
        device_details = await self._miot_connector.get_device_details(self._config.token)
        if device_details is None:
            # Try to find the device by did if token is not unique
            all_devices = await self._miot_connector.get_devices()
            device_details_list = [d for d in all_devices if d.device_id == self._config.device_id]
            if not device_details_list:
                raise DeviceNotFoundException("Device not found")
            device_details = device_details_list[0]

        vacuum_config = VacuumConfig(
            self._miot_connector,
            device_details,
            self._config.server,
            self._config.device_id,
            self._config.host,
            self._config.token,
            self._config.model,
            self._config.colors,
            self._config.drawables,
            self._config.image_config,
            self._config.sizes,
            self._config.texts,
            self._config.store_map_path
        )
        vacuum_class = AVAILABLE_VACUUM_PLATFORMS.get(self._config.used_api, UnsupportedCloudVacuum)
        self._vacuum_connector = vacuum_class(vacuum_config)
