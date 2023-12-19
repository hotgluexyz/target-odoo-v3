"""OdooV2 target sink class, which handles writing streams."""


import json
import xmlrpc.client
from typing import Any, Dict, List, Optional

from dateutil.parser import parse
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import RecordSink

from target_odoo_v3.mapping import UnifiedMapping
import base64
import os.path
from target_hotglue.client import HotglueSink


class OdooV3Sink(HotglueSink):
    """OdooV2 target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.url = self.config.get("url")
        self.db = self.config.get("db")
        self.user = self.config.get("username")
        self.password = self.config.get("password")
        self.uid = self.auth()
        self.so_id = {}
        self.models = None
        self.currencies = None
        self.tax_list = None
        self.tax_group_list = None
        if self.uid == None:
            self.uid = self.auth()

        models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        self.models = models

    def auth(self):
        common = xmlrpc.client.ServerProxy("{}/xmlrpc/2/common".format(self.url))
        return common.authenticate(self.db, self.user, str(self.password), {})

    def query_odoo(self, stream_name, filters):
        return self.models.execute_kw(
            self.db, self.uid, str(self.password), stream_name, "search_read", filters
        )

    def find_parnter(self, parnter_name):
        filters = [[["name", "=", parnter_name]]]
        return self.query_odoo("res.partner", filters)

    def find_product(self, product_name):
        filters = [[["name", "=", product_name]]]
        return self.query_odoo("product.product", filters)

    def find_company(self, name, company_type=None):
        filters = [[["name", "=", name]]]
        if company_type is not None:
            filters[0].append(["company_type", "=", company_type])
        return self.query_odoo("res.partner", filters)

    def find_account(self, name, account_name=None):
        filters = [[["name", "=", name]]]
        if account_name is not None:
            filters[0].append(["name", "=", account_name])
        return self.query_odoo("account.account", filters)

    def find_country(self, name):
        filters = [[["name", "=", name]]]
        return self.query_odoo("res.country", filters)

    def get_odoo_taxes(self, name=None):
        filters = []
        if name is not None:
            filters = [[["name", "=", name]]]

        return self.query_odoo("account.tax", filters)

    def find_currency(self, name):
        filters = []
        if self.currencies is None:
            currencies = {}
            res = self.query_odoo("res.currency", filters)
            if res:
                for currency in res:
                    currencies[currency["name"]] = currency
                self.currencies = currencies

        if name in self.currencies:
            return self.currencies[name]
        else:
            return None

    def _post_odoo(self, stream_name, record, context=None):
        password = str(self._config.get("password"))
        db = self._config.get("db")

        models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        self.models = models
        if context is None:
            context_dictionary = {"lang": "en_US"}
        else:
            context_dictionary = context
        try:
            res = models.execute_kw(
                db,
                self.uid,
                str(password),
                stream_name,
                "create",
                [record],
                {"context": context_dictionary},
            )
            return res
        except xmlrpc.client.Fault as error:
            self.logger.warning(error.faultString)

    # TODO apparently duplicate function was not required. Keeping it for other jobs stability
    def _update_odoo(
        self, stream_name, record, update_id=None, context=None, action="write"
    ):
        password = str(self._config.get("password"))
        db = self._config.get("db")

        models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")
        self.models = models
        if context is None:
            context_dictionary = {"lang": "en_US"}
        else:
            context_dictionary = context
        if update_id:
            record = [[update_id], record]
        else:
            record = [record]
        if action == "action_post":
            record = [[update_id]]
        try:
            res = models.execute_kw(
                db,
                self.uid,
                str(password),
                stream_name,
                action,
                record,
                {"context": context_dictionary},
            )
            return res
        except xmlrpc.client.Fault as error:
            self.logger.warning(error.faultString)

    def read_odoo(self, stream_name, record_id, fields=[]):
        return self.models.execute_kw(
            self.db,
            self.uid,
            str(self.password),
            stream_name,
            "read",
            [[record_id]],
            {"fields": fields},
        )

    def get_tax_list(self):
        if not self.tax_list:
            taxes = self.get_odoo_taxes()
            self.tax_list = {i["name"]: i for i in taxes}
        return self.tax_list

    def get_tax_group_list(self):
        if not self.tax_group_list:
            taxes = self.query_odoo("account.tax.group", [])
            self.tax_group_list = {i["name"]: i for i in taxes}
        return self.tax_group_list

    def get_tax_id(self, tax_name):
        taxes = self.get_tax_list()
        if tax_name in tax_name:
            return taxes[tax_name]
        return {}

    def get_tax_group_id(self, tax_name):
        taxes = self.get_tax_group_list()
        if tax_name in taxes:
            return taxes[tax_name]
        return {}

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record


class TaxRates(OdooV3Sink):
    endpoint = "TaxRates"
    name = "TaxRates"

    def upsert_record(self, record: dict, context: dict):
        taxes = self.get_tax_list()
        groups = self.get_tax_group_list()
        status = True
        state_updates = dict()
        if taxes and groups is not None:
            if bool(record.get("is_percent")):
                amount_type = "percent"
            else:
                # set default tax type
                amount_type = "Fixed"
            if record["name"] not in taxes:
                # default to tax use to purchase for now.
                payload = {
                    "name": record.get("name"),
                    "amount_type": amount_type,
                    "amount": record.get("value"),
                    "type_tax_use": "purchase",
                }
                if record.get("tax_type"):
                    tax_group = self.get_tax_group_id(record.get("tax_type"))
                    if "id" in tax_group:
                        payload.update({"tax_group_id": tax_group["id"]})
                tax_id = self._post_odoo("account.tax", payload)
                if tax_id:
                    state_updates["success"] = True
                else:
                    state_updates["success"] = False
                    status = False
                print(
                    f"TaxRate {record.get('name')} with id {tax_id} added to list of tax rates."
                )

        id = tax_id
        return id, status, state_updates


class Vendors(OdooV3Sink):
    endpoint = "Vendors"
    name = "Vendors"

    def process_vendors(self, record):
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "vendors")
        payload["company_type"] = "company"
        payload["supplier_rank"] = 1
        lookup = self.find_company(payload["name"], payload["company_type"])
        if len(lookup) > 0:
            self.logger.info(f"Supplier {payload['name']} already exists. Skipping...")
            return None
        if payload.get("company_name"):
            company = self.find_company(payload["company_name"])
            if len(company) > 0:
                company = company[0]
                payload["company_id"] = company["id"]
                payload["company_name"] = company["name"]
            else:
                del payload["company_name"]

        if payload.get("country_code"):
            country = self.find_country(payload["country_code"])
            if len(country) > 0:
                country = country[0]
                payload["country_code"] = country["code"]
                payload["country_id"] = country["id"]
            else:
                del payload["country_code"]
        return self._post_odoo("res.partner", payload)

    def upsert_record(self, record: dict, context: dict):
        status = True
        state_updates = dict()

        id = self.process_vendors(record)
        if id:
            state_updates["success"] = True
        else:
            state_updates["success"] = False
            status = False
        return id, status, state_updates


class Suppliers(Vendors):
    endpoint = "Suppliers"
    name = "Suppliers"


class PurchaseInvoices(OdooV3Sink):
    endpoint = "PurchaseInvoices"
    name = "PurchaseInvoices"

    def map_purchase_order(self, record):
        record_processed = {"state": "purchase"}
        # Get the supplier in odoo
        partner = self.find_parnter(record["supplierName"])
        if len(partner) > 0:
            record_processed["partner_id"] = partner[0]["id"]

        # Parse dates into correct format
        due_date = parse(record["dueDate"]).strftime("%Y-%m-%d")
        create_date = parse(record["createdAt"]).strftime("%Y-%m-%d")
        record_processed["create_date"] = due_date
        record_processed["date_order"] = create_date

        # Map invoice name to number
        record_processed["name"] = record["invoiceNumber"]

        return record_processed

    def process_purchase_invoice(self, record):
        record_processed = self.map_purchase_order(record)
        # Create the purchase order
        stream_name = "purchase.order"
        order_id = self._post_odoo(stream_name, record_processed)

        if order_id:
            # Add the line items to the order
            line_items = record.get("lineItems")

            if line_items:
                # If line item is string, convert to dict
                if isinstance(line_items, str):
                    line_items = json.loads(line_items)

                # Build the lines
                for rec in line_items:
                    line_rec = {}
                    line_rec["order_id"] = order_id
                    # Get matching product in Odoo
                    product = self.find_product(rec["productName"])
                    if len(product) > 0:
                        product = product[0]
                        line_rec["product_id"] = product["id"]
                        line_rec["name"] = product["name"]
                        line_rec["price_unit"] = rec["unitPrice"]
                        line_rec["product_qty"] = rec["quantity"]
                        line_rec["price_total"] = rec["totalPrice"]
                        if rec.get("product_uom_qty"):
                            line_rec["product_uom_qty"] = int(rec["product_uom_qty"])
                        # Post the line to Odoo
                        self._post_odoo(f"{stream_name}.line", line_rec)
        return order_id

    def upsert_record(self, record: dict, context: dict):
        status = True
        state_updates = dict()

        id = self.process_purchase_invoice(record)
        if id:
            state_updates["success"] = True
        else:
            state_updates["success"] = False
            status = False
        return id, status, state_updates


class Invoices(OdooV3Sink):
    endpoint = "Invoices"
    name = "Invoices"

    def get_line_items(self, invoice_id):
        return self.models.execute_kw(
            self.db,
            self.uid,
            str(self.password),
            "account.move.line",
            "search_read",
            [[("move_id", "=", invoice_id)]],
            {
                "fields": [
                    "id",
                    "name",
                    "product_id",
                    "quantity",
                    "price_unit",
                    "account_id",
                ]
            },
        )

    def get_invoice_attachments(self, invoice_id):
        invoice = self.read_odoo(
            "account.move", invoice_id, ["id", "name", "attachment_ids"]
        )
        if invoice:
            attachment_ids = invoice[0].get("attachment_ids", [])

            # Retrieve the attachment records
            attachments = self.models.execute_kw(
                self.db,
                self.uid,
                str(self.password),
                "ir.attachment",
                "search_read",
                [[("id", "in", attachment_ids)]],
                {"fields": ["id", "name"]},
            )
            return attachments
        return []

    def upload_attachment(self, record_id, document_id, document_name):
        input_path = self.config.get("input_path", "./")
        file_name = os.path.join(input_path, f"{document_id}_{document_name}")
        if os.path.isfile(file_name):
            with open(file_name, "rb") as f:
                document_content = f.read()
            # document_content = xmlrpc.client.Binary(document_content)
            document_content = base64.b64encode(document_content).decode("utf-8")

            payload = {
                "name": f"{document_id}_{document_name}",
                "datas": document_content,
                "res_model": "account.move",
                "res_id": record_id,
            }
            attachment = self._post_odoo("ir.attachment", payload)
            return attachment

    def map_invoice(self, record, contact_key):
        record_processed = {"state": record["status"].lower()}
        # Get the supplier in odoo
        partner = self.find_parnter(record[contact_key])
        if len(partner) > 0:
            record_processed["partner_id"] = partner[0]["id"]

        # Parse dates into correct format
        due_date = parse(record["dueDate"]).strftime("%Y-%m-%d")
        create_date = parse(record["createdAt"]).strftime("%Y-%m-%d")
        record_processed["invoice_date"] = create_date
        record_processed["invoice_date_due"] = due_date

        # Map invoice name to number
        record_processed["name"] = record["invoiceNumber"]

        return record_processed

    def process_invoice(
        self, record, inv_type="out_invoice", contact_key="customerName"
    ):
        stream_name = "account.move"
        record_processed = self.map_invoice(record, contact_key)
        # Don't wish to affect Invoices stream yet.
        record_processed["ref"] = record_processed["name"]
        del record_processed["name"]
        mark_posted = False
        if record_processed.get("state") == "posted":
            record_processed["state"] = "draft"
            mark_posted = True
        context_dictionary = None
        currency_id = self.find_currency(record["currency"])
        if currency_id is None:
            print("Currency not found. Skipping..")
            return
        currency_id = currency_id["id"]
        record_processed["move_type"] = inv_type
        record_processed["payment_state"] = "not_paid"
        record_processed["currency_id"] = currency_id
        # if record.get("id"):
        #     order_id = record.get("id")
        #     self._update_odoo(stream_name, record=record_processed, update_id=order_id)
        # else:
        # Create the Invoice
        order_id = self._post_odoo(stream_name, record_processed)
        lines = []
        if order_id:
            # Handle attachments
            if record.get("attachments"):
                # If line item is string, convert to dict
                if isinstance(record["attachments"], str):
                    record["attachments"] = json.loads(record["attachments"])
                attachments = self.get_invoice_attachments(order_id)

                for attachment in record["attachments"]:
                    existing_attachment = next(
                        (
                            inv_attachment
                            for inv_attachment in attachments
                            if inv_attachment["name"] == attachment.get("name")
                        ),
                        None,
                    )
                    # If Attachment already exists, no need to upload again
                    if not existing_attachment:
                        self.upload_attachment(
                            order_id, attachment.get("id"), attachment.get("name")
                        )
            # Add the line items to the order
            line_items = record.get("lineItems")

            if line_items:
                existing_line_items = self.get_line_items(order_id)
                # Create a dictionary of existing line items for easy lookup
                existing_line_items_dict = {
                    item["name"]: item for item in existing_line_items
                }

                # If line item is string, convert to dict
                if isinstance(line_items, str):
                    line_items = json.loads(line_items)

                # Build the lines
                for rec in line_items:
                    line_rec = {}
                    line_rec["move_id"] = order_id
                    # Get matching product in Odoo
                    product = self.find_product(rec["productName"])
                    if len(product) > 0:
                        product = product[0]
                    else:
                        product = {}

                    if rec.get("accountName"):
                        account_id = self.find_account(rec["accountName"])
                    else:
                        account_id = []
                    if len(account_id) == 0:
                        print("Valid Account name required. Skipping..")
                        # skip the line
                        continue
                    account_id = account_id[0]["id"]
                    if product.get("id"):
                        line_rec["product_id"] = product.get("id")

                    if product.get("name"):
                        line_rec["name"] = product.get("name")
                    elif rec.get("productName"):
                        line_rec["name"] = rec.get("productName")
                    line_rec["price_unit"] = rec.get("unitPrice")
                    line_rec["quantity"] = rec.get("quantity")
                    line_rec["price_subtotal"] = rec.get("totalPrice")
                    line_rec["discount"] = rec.get("discountAmount", 0)

                    if "displayType" in rec:
                        line_rec["display_type"] = rec["displayType"]
                        if rec["displayType"] is False:
                            context_dictionary = {
                                "lang": "en_US",
                                "check_move_validity": False,
                            }
                    else:
                        # Default to product according to unified schema
                        line_rec["display_type"] = "product"

                    # TODO map these when required.
                    # line_rec["debit"] = 1
                    # line_rec["credit"] = 0
                    # line_rec["tax_repartition_line_id"] = False
                    # line_rec["tax_exigible"] = False
                    # line_rec["recompute_tax_line"] = False
                    # line_rec["predict_from_name"] = False
                    # line_rec["is_rounding_line"] = False
                    # line_rec["exclude_from_invoice_tab"] = False
                    # line_rec["account_internal_type"] = "other"
                    # line_rec["account_internal_group"] = "expense"

                    line_rec["currency_id"] = currency_id
                    if rec.get("taxCode"):
                        tax_detail = self.get_tax_id(rec.get("taxCode"))
                        if "id" in tax_detail:
                            line_rec["tax_ids"] = [tax_detail["id"]]  # [3,34]

                    line_rec["account_id"] = account_id
                    if rec.get("product_uom_qty"):
                        line_rec["product_uom_qty"] = int(rec["product_uom_qty"])

                    if line_rec["name"] in existing_line_items_dict:
                        line_item_id = existing_line_items_dict[line_rec["name"]]["id"]
                        # Update line item.
                        self._update_odoo(f"{stream_name}.line", line_rec, line_item_id)
                    else:
                        # Post the line to Odoo
                        self._post_odoo(
                            f"{stream_name}.line", line_rec, context_dictionary
                        )
            if mark_posted:
                updated = self._update_odoo(
                    stream_name, record={"state": "posted"}, update_id=order_id,
                    action="action_post"
                )
                if updated:
                    # We need to verify that bill/invoice is not marked as paid.
                    invoice_due_amount = self.read_odoo(
                        "account.move", order_id, ["amount_residual"]
                    )
                    if len(invoice_due_amount) > 0:
                        invoice_due_amount = invoice_due_amount[0]
                        if "amount_residual" in invoice_due_amount:
                            if invoice_due_amount["amount_residual"] <= 0:
                                # Manually override the Paid status.
                                updated_amount = self._update_odoo(
                                    stream_name,
                                    record={"payment_state": "not_paid"},
                                    update_id=order_id,
                                )
                    print(f"Invoice {order_id} marked as Posted")
        return order_id

    def upsert_record(self, record: dict, context: dict):
        status = True
        state_updates = dict()

        id = self.process_invoice(record)
        if id:
            if record.get("id"):
                state_updates["is_updated"] = True
            else:
                state_updates["success"] = True
        else:
            state_updates["success"] = False
            status = False
        return id, status, state_updates


class Bills(Invoices):
    endpoint = "Bills"
    name = "Bills"

    def upsert_record(self, record: dict, context: dict):
        status = True
        state_updates = dict()

        id = self.process_invoice(
            record, inv_type="in_invoice", contact_key="vendorName"
        )
        if id:
            if record.get("id"):
                state_updates["is_updated"] = True
            else:
                state_updates["success"] = True
        else:
            state_updates["success"] = False
            status = False
        return id, status, state_updates
