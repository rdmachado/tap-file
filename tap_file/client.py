"""Custom client handling, including FileStream base class."""

from __future__ import annotations

from os import PathLike
import typing as t
from azure.storage.blob import BlobServiceClient, ContainerClient
from singer_sdk import Tap
from singer_sdk.singerlib.schema import Schema
from singer_sdk.streams import Stream
from singer_sdk import typing as th 
from smart_open import open
import pandas as pd
from io import StringIO
import csv, uuid, os, re
from urllib.parse import urlparse

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

class File:

    def __init__(self, file_path: str, encoding: str = 'utf8', transport_params = None, offline = False) -> None:
        self.file_path = file_path
        self.transport_params = transport_params
        self.fd = None
        self.encoding = "cp1252" #encoding
        self.offline = offline
        self.temp_files_path = None

    def __del__(self):
        self.fd.close()
        if self.temp_files_path and os.path.exists(self.temp_files_path):
            os.remove(self.temp_files_path)

    def read(self, n_rows: int = 1000) -> list[str]:
        if not self.fd:
            self._open_file()

        rows = []
        for i in range(n_rows):
            l = self.fd.readline()
            if not l:
                break
            rows.append(l)

        return rows

    def peek(self, n_rows: int = 1000) -> list[str]:
        if not self.fd:
            self._open_file()

        p_cursor = self.fd.tell()
        
        rows = self.read(n_rows)
        
        self.fd.seek(p_cursor)
        
        return rows
    
    def skip(self, n_rows: int = 1) -> None:
        if not self.fd:
            self._open_file()

        for i in range(n_rows):
            self.fd.readline()


    def _open_file(self):
        file_path = self.file_path

        if self.offline:
            self.temp_files_path = f'./{str(uuid.uuid4())}'
            file_path = self.temp_files_path
            with open(self.file_path, encoding=self.encoding, transport_params=self.transport_params) as src:
                with open(self.temp_files_path, mode='w', encoding=self.encoding) as out:
                    out.write(src.read())
                    self.fd = out
        
        self.fd = open(file_path, encoding=self.encoding, transport_params=self.transport_params)


class CSVStream(Stream):
    """Stream class for CSV file streams."""

    def __init__(self, *args, **kwargs) -> None:
        self.file_config = kwargs.pop("file_config")
        self.provider_config = kwargs.pop("provider_config")
        self.transport_params = self._build_transport_params()
        self.header = None
        self._schema = dict()
        self.files = self._find_matching_files()

        super().__init__(*args, **kwargs)
    
    def _build_transport_params(self):
        t_params = {}
        match self.provider_config['name']:
            case 'azureblobstorage':
                t_params['client'] = BlobServiceClient.from_connection_string(self.provider_config['azureblobstorage_connection_string'])
            case 'https':
                pass
        return t_params
                
    def _find_matching_files(self):
        if not self.file_config['regex']:
            return [File(self.file_config['url'], transport_params=self.transport_params, offline=self.file_config['offline'])]
        else:
            files = []
            pr = urlparse(self.file_config['url'])
            exp = re.compile(self.file_config['regex'])
            match self.provider_config['name']:
                case 'azureblobstorage':
                    cc = ContainerClient.from_connection_string(self.provider_config['azureblobstorage_connection_string'], pr.netloc)
                    matching_filenames = [f for f in cc.list_blob_names(name_starts_with=pr.path) if exp.fullmatch(f)]
                    for f in matching_filenames:
                        files.append(File(os.path.join(self.file_config['url'], f), transport_params=self.transport_params, offline=self.file_config['offline']))
                case 'https':
                    raise NotImplementedError()
            return files


    @property
    def schema(self):

        if not self._schema:

            properties: list[th.Property] = []

            str_rows = ''.join(self.files[0].peek())
            self._dialect = csv.Sniffer().sniff(str_rows)

            dtype = 'str'
            if self.file_config['infer_data_types']:
                dtype = None

            df = pd.read_csv(StringIO(str_rows), dialect=self._dialect, dtype=dtype)
            table_schema = pd.io.json.build_table_schema(df, index=False)
            type_translation = {
                'integer': th.IntegerType(),
                'string': th.StringType(),
                'boolean': th.BooleanType(),
                'number': th.NumberType(),
                'datetime': th.DateTimeType(),
                'duration': th.DurationType(),
            }
            
            for field in table_schema['fields']:
                properties.append(th.Property(field['name'], type_translation[field['type']]))

            # cache header
            self._header = [f['name'] for f in table_schema['fields']]

            # cache schema
            self._schema = th.PropertiesList(*properties).to_dict()
            self._dtypes = df.dtypes

        return self._schema

    def get_records(
            self,
            context: Context | None
    ) -> t.Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument."""
        
        for file in self.files:
            file.skip() # skip header
            while rows := file.read(n_rows=10000):
                df = pd.read_csv(StringIO(''.join(rows)), dialect=self._dialect, names=self._header, dtype=self._dtypes.to_dict())
                json_lines = df.to_dict(orient='records')
                for line in json_lines:
                    yield line
