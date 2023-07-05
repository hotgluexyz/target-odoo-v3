"""OdooV2 target class."""

from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue


from target_odoo_v3.sinks import (
    TaxRates,
    Vendors,
    Suppliers,
    PurchaseInvoices,
    Invoices,
    Bills,
)


class TargetOdooV3(TargetHotglue):
    SINK_TYPES = [
        TaxRates,
        Vendors,
        Suppliers,
        PurchaseInvoices,
        Invoices,
        Bills,
    ]
    name = "target-odoo-v3"
    config_jsonschema = th.PropertiesList(
        th.Property("db", th.StringType, required=True),
        th.Property("url", th.StringType, required=True),
        th.Property("username", th.StringType, required=True),
        th.Property("password", th.StringType, required=True),
    ).to_dict()


if __name__ == "__main__":
    TargetOdooV3.cli()
