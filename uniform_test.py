from pyiceberg.catalog import load_catalog
import logging


def main():
    rest_catalog = load_catalog(
    "databricks",
    **{
        "type": "rest",
        "warehouse": "unity",
        "uri": "http://localhost:8080/api/2.1/unity-catalog/iceberg",
    }
    )
    print(rest_catalog.list_namespaces())
    print(rest_catalog.list_tables("default"))
    print(rest_catalog.load_table("default.marksheet_uniform").scan().to_pandas())


if __name__ == "__main__":
    main()