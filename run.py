"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    DoubleType,
    DateType,
    BooleanType,
)
DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())



def ingest(data_dir: Path, db: Path) -> None:
    """Load source tables and supplier price files into raw schema."""
    with duckdb.connect(str(db)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        sqlite_path = (data_dir / "qafila_oltp.sqlite").resolve()
        con.execute("INSTALL sqlite")
        con.execute("LOAD sqlite")

        sqlite_path_sql = str(sqlite_path).replace("'", "''")
        con.execute(
            f"ATTACH '{sqlite_path_sql}' AS app (TYPE SQLITE, READ_ONLY)"
        )

        try:
            for table in ("orders", "order_items", "products", "stores"):
                con.execute(
                    f"CREATE OR REPLACE TABLE raw.{table} "
                    f"AS SELECT * FROM app.{table}"
                )
        finally:
            con.execute("DETACH app")

        supplier_glob = (data_dir / "suppliers" / "prices_*.csv").as_posix()
        con.execute("""
            CREATE OR REPLACE TABLE raw.supplier_prices AS
            SELECT
                * EXCLUDE (filename),
                regexp_extract(filename, '[^/]+$') AS source_file
            FROM read_csv(
                ?,
                filename = true,
                union_by_name = true,
                all_varchar = true
            )
        """, [supplier_glob])


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def iceberg_schema(arrow_schema):
    import pyarrow.types as pa_types

    fields = []
    for field_id, field in enumerate(arrow_schema, start=1):
        t = field.type

        if pa_types.is_string(t) or pa_types.is_large_string(t):
            iceberg_type = StringType()
        elif pa_types.is_boolean(t):
            iceberg_type = BooleanType()
        elif pa_types.is_date(t):
            iceberg_type = DateType()
        elif pa_types.is_floating(t) or pa_types.is_decimal(t):
            iceberg_type = DoubleType()
        elif pa_types.is_integer(t):
            iceberg_type = LongType()
        else:
            raise TypeError(f"Unsupported Arrow type for {field.name}: {t}")

        fields.append(
            NestedField(
                field_id=field_id,
                name=field.name,
                field_type=iceberg_type,
                required=False,
            )
        )

    return Schema(*fields)


def publish(db: Path, warehouse: Path) -> None:
    """Publish dbt models to Iceberg while preserving snapshot history."""
    catalog = get_catalog(warehouse)
    models = {
        "silver": "product_cost_history",
        "gold": "weekly_margin",
        "quality": "price_quarantine",
    }

    for namespace in models:
        catalog.create_namespace_if_not_exists(namespace)

    namespaces = {ns[0] for ns in catalog.list_namespaces() if len(ns) == 1}

    with duckdb.connect(str(db), read_only=True) as con:
        for namespace, model in models.items():
            if namespace not in namespaces:
                catalog.create_namespace(namespace)
                namespaces.add(namespace)

            identifier = f"{namespace}.{model}"
            data = con.execute(
                f"SELECT * FROM {namespace}.{model}"
            ).fetch_arrow_table()
            if namespace == "gold":
                import pyarrow as pa
                column_index = data.schema.get_field_index("uncosted_lines")
                column = data.column("uncosted_lines").cast(pa.float64())
                data = data.set_column(
                    column_index,
                    pa.field("uncosted_lines", pa.float64()),
                    column
                )
            if catalog.table_exists(identifier):
                table = catalog.load_table(identifier)
                table.overwrite(data)
            else:
                table = catalog.create_table(
                    identifier=identifier,
                    schema=iceberg_schema(data.schema),
                )
                table.append(data)

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
