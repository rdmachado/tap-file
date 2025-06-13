"""File tap class."""

from __future__ import annotations

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_file.client import CSVStream


class TapFile(Tap):
    """File tap class."""

    name = "tap-file"

    config_jsonschema = th.PropertiesList(
        th.Property("files",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("name",
                                        th.StringType(nullable=False),
                                        required=True,
                                        title="Stream name",
                                        description="Name used to name the stream when passed to the target."
                            ),
                            th.Property("url", 
                                        th.StringType(nullable=False), 
                                        required=True, 
                                        title="URL", 
                                        description="The path to the file(s).", 
                                        examples=["local://mydir/myfile.csv", "https://www.mysite.com/myfile.csv", "azure://container/mydir/myfile.csv"]
                            ),
                            th.Property("format",
                                        th.StringType(allowed_values=["csv", "parquet"],
                                                      examples=["csv", "parquet"]
                                        )
                            )
                        )
                    )
        ),
        
        th.Property("provider",
                    th.ObjectType(
                        th.Property("name",
                                    th.StringType(allowed_values=["local", "http", "https", "azureblobstorage"],
                                                    examples=["https", "azureblobstorage"]
                                    )
                        ),
                        th.Property("azureblobstorage_connection_string",
                                    th.StringType(nullable=False),
                                    title="Azure Blob Storage connection string",
                                    description="The connection string to reach and authenticate with the Azure Blob Storage service.")
                    )
        )
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        streams = []
        for file in self.config.get('files'):
            match file['format']:
                case 'csv':
                    streams.append(
                        CSVStream(
                            tap=self,
                            name=file['name'],
                            file_config=file,
                            provider_config=self.config.get('provider')
                        )
                    ) 
        return streams



if __name__ == "__main__":
    TapFile.cli()
