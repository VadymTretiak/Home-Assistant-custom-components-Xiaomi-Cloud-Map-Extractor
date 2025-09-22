from __future__ import annotations

from homeassistant.config_entries import OptionsFlow

from .types import XiaomiCloudMapExtractorConfigEntry


class XiaomiCloudMapExtractorOptionsFlowHandler(OptionsFlow):
    """Handle an options flow for Xiaomi Cloud Map Extractor."""

    def __init__(self, config_entry: XiaomiCloudMapExtractorConfigEntry) -> None:
        """Initialize options flow."""
        self.config_entry = config_entry

    async def async_step_init(self, user_input=None):
        """Manage the options."""
        # For now, we don't have any options to configure after setup.
        # This can be expanded in the future.
        return self.async_create_entry(title="", data={})
