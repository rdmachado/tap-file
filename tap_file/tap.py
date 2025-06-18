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
                                        ),
                                        required=True,
                                        title="File format"
                            ),
                            th.Property("encoding",
                                        th.StringType(),
                                        default="utf8",
                                        title="Text encoding",
                                        description="Specify file encoding, if not specified defaults to utf-8"
                            ),
                            th.Property("infer_data_types",
                                        th.BooleanType(),
                                        default=False,
                                        title="Infer data type",
                                        description="Whether to try to infer field types or treat all fields as string."
                            ),
                            th.Property("offline",
                                        th.BooleanType(),
                                        default=False,
                                        title="Offline mode",
                                        description="Download entire file before processing the data. This can be useful to prevent timeout issues with the source when there is enough disk space."
                            ),
                            th.Property("regex",
                                        th.StringType(),
                                        default="",
                                        title="Regex pattern",
                                        description="When specified will treat the url as a directory and extract files that match the file name pattern."
                            ),
                            th.Property("format_options",
                                        th.ObjectType(
                                            th.Property("csv_delimiter",
                                                        th.StringType(),
                                                        default="",
                                                        title="CSV Delimiter",
                                                        Description="Will be autodetected if not specified."
                                            ),
                                            th.Property("csv_escapechar",
                                                        th.StringType(),
                                                        default="",
                                                        title="CSV Escape Character",
                                                        Description="Will be autodetected if not specified."
                                            ),
                                            th.Property("csv_lineterminator",
                                                        th.StringType(),
                                                        default="",
                                                        title="CSV Line Terminator",
                                                        Description="Will be autodetected if not specified."
                                            ),
                                            th.Property("csv_quotechar",
                                                        th.StringType(),
                                                        default="",
                                                        title="CSV Quote Character",
                                                        Description="Will be autodetected if not specified."
                                            )
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
                                    default="",
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
