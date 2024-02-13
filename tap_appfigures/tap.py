"""AWin tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_appfigures.streams import (
    ProductsStream,
    SubscriptionsStream,
    # RevenueStream,
    # SalesStream,
)

STREAM_TYPES = [
    ProductsStream,
    SubscriptionsStream,
    # RevenueStream,
    # SalesStream,
]


class TapAppFigures(Tap):
    name = "tap-appfigures"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "pat",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2016-01-01T00:00:00Z",
            description="The earliest transaction date to sync"
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            default="2016-01-01T00:00:00Z",
            description="The oldest transaction date to sync"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
