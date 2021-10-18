"""
Types/functions used by both the reader and writer.

Overview of overall data structure layout:

    TableVersionsServer:
        # with history
        userspace, table_name -> table_id
        # with history
        table_id -> TableVersion(table_schema_filename, data_list_filename,
                                 version_number, branch_spec)

    # convention is table_schema.{table_id}.{uuid}
    table_schema_filename -> TableSchema(
        columns_names_and_types: NOT IMPLEMENTED,
        deduplication_keys: # list of column names to use as the key for deduplicating
            rows

    # convention is data_list.{table_id}.{uuid}
    data_list_filename -> List[DataFileEntry(data_file_type, data_filename)]

    data_file_type -> 'write' | 'delete'

    # convention is {write|delete}.{table_id}.{uuid}]
    data_filename -> DataFrame

"""

from __future__ import annotations

from typing import List, Literal, Optional
from dataclasses import dataclass
import uuid


_dummy_uuid = uuid.UUID(int=0)


@dataclass(frozen=True, order=True)
class TableName:
    """A TableName maps a userspace/table name to a table id"""

    # This needs to be the first field! We rely on this being the first field and
    # therefore effectively defining the sort order
    version_number: int
    userspace: str
    table_name: str
    table_id: uuid.UUID

    @staticmethod
    def dummy(version_number: int) -> TableName:
        """
        The bisect library doesn't allow for passing in a sort key, so we just make
        TableName sortable based on version_number (which must be the first field in the
        class in order to make dataclass' built-in ordering functions work this way).
        This means we sometimes need dummy TableNames to compare against. This function
        constructs those dummy TableNames.
        """
        return TableName(version_number, "", "", _dummy_uuid)


@dataclass(frozen=True, order=True)
class TableVersion:
    """A TableVersion represents a single version of a table"""

    # This needs to be the first field! We rely on this being the first field and
    # therefore effectively defining the sort order
    version_number: int

    # The table_versions server maps table_ids to a userspace/table name. A single
    # table_id can map to 0, 1, 2 or more userspace/table names.
    # TODO should it be possible to have a table_id map to prod/foo AND user1/bar? Seems
    #  too confusing...
    table_id: uuid.UUID

    # Points to a file which contains a TableSchema
    table_schema_filename: str

    # Points to a file which contains an ordered list of DataFileEntry
    data_list_filename: str

    @staticmethod
    def dummy(version_number: int) -> TableVersion:
        """See TableName.dummy"""
        return TableVersion(version_number, _dummy_uuid, "", "")


@dataclass(frozen=True)
class TableSchema:
    # TODO currently not used
    column_names_and_types: None

    # If this is not None, rows will automatically be deduplicated (newest rows
    # preserved) based on the columns specified by deduplication_keys. E.g. if
    # deduplication_keys for table t is ['a', 'b'], this means that when you write to t,
    # with a = 1, b = 2, any existing rows in t where a = 1 and b = 2 will be
    # effectively deleted. As a result, when you read from t, every row will have a
    # distinct deduplication key. If deduplication_keys is None, then new rows are
    # appended normally.
    deduplication_keys: Optional[List[str]]


@dataclass(frozen=True)
class DataFileEntry:
    # Can be "write", "delete", or "delete_all". "write" can be interpreted as an append
    # or an upsert depending on the table's deduplication_keys. "delete" is a delete
    # where equal (see Connection.delete_where_equal). "delete_all" means all existing
    # data was deleted at that point. "delete_all" is necessary (instead of just having
    # an empty data_list) so that a table in a userspace can ignore data in a parent
    # userspace.
    data_file_type: Literal["write", "delete", "delete_all"]

    # Points to a parquet file. Will be None only if data_file_type == "delete_all"
    data_filename: Optional[str]

    # TODO add some statistics here to make querying faster