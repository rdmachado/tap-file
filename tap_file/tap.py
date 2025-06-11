"""File tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_file import streams


class TapFile(Tap):
    """File tap class."""

    name = "tap-file"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "azure_storage_account_connection_string",
            th.StringType(nullable=False),
            required=False,
            secret=True,  # Flag config as protected.
            title="Azure Storage Account Connection String",
            description="The connection string for the Azure Storage Account",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType(nullable=False), nullable=False),
            required=True,
            title="Project IDs",
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.FileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupsStream(self),
            streams.UsersStream(self),
        ]


if __name__ == "__main__":
    TapFile.cli()
