from pyiceberg.catalog import load_catalog
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection
from pyiceberg.expressions import EqualTo
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

import pandas as pd
import pyarrow as pa


if __name__ == '__main__':
    warehouse_path = "tmp/warehouse"
    catalog = load_catalog(
        "default",
        **{
            'type': 'sql',
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    catalog.create_namespace_if_not_exists("default")

    schema = Schema(
        NestedField(1, "Register Value", IntegerType(), required=True),
        NestedField(2, "Player Name", StringType(), required=True),
        NestedField(3, "Salary in $", IntegerType(), required=True),
        NestedField(4, "Season Start", IntegerType(), required=True),
        NestedField(5, "Season End", IntegerType(), required=True),
        NestedField(6, "Team", StringType(), required=True),
        NestedField(7, "Full Team Name", StringType(), required=True),
        identifier_field_ids=[1],
    )

    tbl = catalog.create_table_if_not_exists(
        "default.players",
        schema=schema,
        partition_spec=PartitionSpec(PartitionField(source_id=4, field_id=4004, transform=IdentityTransform(), name="Season Start")),
        sort_order=SortOrder(SortField(source_id=3, transform=IdentityTransform(), direction=SortDirection.DESC)),
    )

    arrow_schema = pa.schema(
        [
            pa.field("Register Value", pa.int32(), nullable=False),
            pa.field("Player Name", pa.string(), nullable=False),
            pa.field("Salary in $", pa.int32(), nullable=False),
            pa.field("Season Start", pa.int32(), nullable=False),
            pa.field("Season End", pa.int32(), nullable=False),
            pa.field("Team", pa.string(), nullable=False),
            pa.field("Full Team Name", pa.string(), nullable=False),
        ]
    )

    df = pd.read_excel("data/data.xlsx", header=0)
    df = pa.Table.from_pandas(df, arrow_schema)
    tbl.upsert(df)

    print("\n---\nData for 2016 year\n---\n", tbl.scan(
        row_filter=EqualTo("Season Start", 2016),
    ).to_pandas())

    print("\n---\nTop 10 the most expensive players by 2016\n---\n", tbl.scan(
        row_filter=EqualTo("Season Start", 2016),
    ).to_pandas().sort_values(by="Salary in $", ascending=False).head(10))
