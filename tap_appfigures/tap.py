"""AWin tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_appfigures.streams import (
    ProductsStream,
    # TransactionsStream,
    # PublishersStream,
    # ReportByPublisherStream,
)

STREAM_TYPES = [
    ProductsStream,
    # TransactionsStream,
    # PublishersStream,
    # ReportByPublisherStream,
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
            "properties",
            th.StringType,
            default="Legacy",
            description="Legacy cmd line arg"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
