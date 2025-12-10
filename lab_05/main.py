from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import EqualTo, And
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField, IdentityTransform
from pyiceberg.types import IntegerType, NestedField, StringType

import pandas as pd
import pyarrow as pa
from pathlib import Path


if __name__ == '__main__':
    warehouse_path = "tmp/warehouse"
    abs_path = Path(__file__).parent.resolve() / warehouse_path
    abs_path.mkdir(parents=True, exist_ok=True)
    
    catalog = load_catalog(
        "default",
        **{
            'type': 'sql',
            "uri": f"sqlite:///{abs_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{abs_path}",
        },
    )
    catalog.create_namespace_if_not_exists("default")

    df = pd.read_excel("data/data.xlsx", header=0)
    
    if not catalog.table_exists("default.players"):
        arrow_schema = pa.schema([
            pa.field("Register Value", pa.int32(), nullable=False),
            pa.field("Player Name", pa.string(), nullable=False),
            pa.field("Salary in $", pa.int32(), nullable=False),
            pa.field("Season Start", pa.int32(), nullable=False),
            pa.field("Season End", pa.int32(), nullable=False),
            pa.field("Team", pa.string(), nullable=False),
            pa.field("Full Team Name", pa.string(), nullable=False),
        ])
        
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
        
        catalog.create_table(
            "default.players",
            schema=schema,
            partition_spec=PartitionSpec(PartitionField(source_id=4, field_id=4004, transform=IdentityTransform(), name="Season Start")),
        )
    
    tbl = catalog.load_table("default.players")
    years = list(range(1990, 2018))
    
    for y in years:
        print(f"\nProcessing year {y}...")
        
        updates_df = df[df['Season Start'] == y].copy()
        
        if y == years[0]:
            final_df = updates_df.copy()
        else:
            prev_year = y - 1
            try:
                prev_scan = tbl.scan(row_filter=EqualTo("Season Start", prev_year))
                prev_df = prev_scan.to_pandas()
                
                if len(prev_df) == 0:
                    print(f"  Warning: No data in previous year ({prev_year})")
                    final_df = updates_df.copy()
                else:
                    prev_df['Season Start'] = y
                    
                    merged_df = prev_df.copy()
                    
                    for _, update_row in updates_df.iterrows():
                        reg_val = update_row['Register Value']
                        merged_df = merged_df[merged_df['Register Value'] != reg_val]
                        merged_df = pd.concat([merged_df, pd.DataFrame([update_row])], ignore_index=True)
                    
                    final_df = merged_df
            except Exception as e:
                print(f"  Warning: Could not read previous year ({prev_year}): {e}")
                final_df = updates_df.copy()
        
        if len(final_df) > 0:
            final_df = final_df.reset_index(drop=True)
            
            final_df["Register Value"] = final_df["Register Value"].astype("int32")
            final_df["Salary in $"] = final_df["Salary in $"].astype("int32")
            final_df["Season Start"] = final_df["Season Start"].astype("int32")
            final_df["Season End"] = final_df["Season End"].astype("int32")
            
            arrow_schema = pa.schema([
                pa.field("Register Value", pa.int32(), nullable=False),
                pa.field("Player Name", pa.string(), nullable=False),
                pa.field("Salary in $", pa.int32(), nullable=False),
                pa.field("Season Start", pa.int32(), nullable=False),
                pa.field("Season End", pa.int32(), nullable=False),
                pa.field("Team", pa.string(), nullable=False),
                pa.field("Full Team Name", pa.string(), nullable=False),
            ])
            arrow_table = pa.Table.from_pandas(final_df, schema=arrow_schema, preserve_index=False)
            
            tbl.append(arrow_table)
            print(f"  Loaded {len(final_df)} records for year {y}")
        else:
            print(f"  No data for year {y}")
    
    print("\n---\nData for 2016 year\n---\n")
    data2016 = tbl.scan(
        row_filter=EqualTo("Season Start", 2016),
    ).to_pandas()
    print(data2016.sort_values(by="Player Name"))
    
    print("\n---\nTop 10 the most expensive players by 2016\n---\n")
    top10 = data2016.sort_values(by="Salary in $", ascending=False).head(10)
    print(top10[["Register Value", "Player Name", "Salary in $", "Team"]].to_string(index=False))
