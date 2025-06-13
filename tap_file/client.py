"""Custom client handling, including FileStream base class."""

from __future__ import annotations

from os import PathLike
import typing as t
from azure.storage.blob import BlobServiceClient
from singer_sdk import Tap
from singer_sdk.singerlib.schema import Schema
from singer_sdk.streams import Stream
from singer_sdk import typing as th 
from smart_open import open
import pandas as pd
from io import StringIO

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

class File:

    def __init__(self, file_path: str, encoding: str = 'utf8', transport_params = None) -> None:
        self.file_path = file_path
        self.transport_params = transport_params
        self.fd = None
        self.encoding = encoding
        
    def peek(self, n_rows: int = 1000) -> list[str]:
        rows = []
        for i in range(n_rows):
            rows.append(self.readline())
        self.fd.seek(0)
        return rows

    def readline(self):
        if not self.fd:
            self._open_file()
    
        return self.fd.readline()

    def _open_file(self):
        self.fd = open(self.file_path, encoding=self.encoding, transport_params=self.transport_params)


class CSVStream(Stream):
    """Stream class for File streams."""

    def __init__(self, *args, **kwargs) -> None:
        self.file_config = kwargs.pop("file_config")
        self.provider_config = kwargs.pop("provider_config")
        self.transport_params = self._build_transport_params()
        self.header = None
        self._schema = dict()
        self.file = File(self.file_config['url'], transport_params=self.transport_params)

        super().__init__(*args, **kwargs)
    
    def _build_transport_params(self):
        t_params = {}
        match self.provider_config['name']:
            case 'azureblobstorage':
                t_params['client'] = BlobServiceClient.from_connection_string(self.provider_config['azureblobstorage_connection_string'])
        
        return t_params
                
    @property
    def schema(self):

        if not self._schema:

            properties: list[th.Property] = []

            rows = self.file.peek()
            df = pd.read_csv(StringIO(''.join(rows)))
            schema = pd.io.json.build_table_schema(df, index=False)
            type_translation = {
                'integer': th.IntegerType(),
                'string': th.StringType(),
                'boolean': th.BooleanType(),
                'number': th.NumberType(),
                'datetime': th.DateTimeType(),
                'duration': th.DurationType(),
            }
            
            for field in schema['fields']:
                properties.append(th.Property(field['name'], type_translation[field['type']]))

            # cache header
            self.header = [f['name'] for f in schema['fields']]

            # cache schema
            self._schema = th.PropertiesList(*properties).to_dict()

        return self._schema

    def get_records(
            self,
            context: Context | None
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument."""

        yield dict(zip(self.header, self.file.readline()))


