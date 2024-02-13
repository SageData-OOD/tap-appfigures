"""Stream type classes for tap-appfigures."""

import datetime
import json
from urllib.parse import urlparse
from urllib.parse import parse_qs
from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_appfigures.client import AppFiguresStream

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


class ProductsStream(AppFiguresStream):
    name = "products"
    path = "/products/mine"
    primary_keys = ["id"]
    # replication_key = "updated_date"
    records_jsonpath = "$.*"
    schema = th.PropertiesList(
        th.Property(
            "id",
            th.IntegerType,
            # description="The Account's ID"
        ),
        th.Property(
            "name",
            th.StringType,
            # description="Given name for the account"
        ),
        th.Property(
            "developer",
            th.StringType,
            # description="Type of account"
        ),
        th.Property(
            "icon",
            th.StringType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "vendor_identifier",
            th.StringType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "ref_no",
            th.IntegerType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "sku",
            th.StringType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "store_id",
            th.IntegerType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "store",
            th.StringType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "release_date",
            th.DateType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "added_date",
            th.DateType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "updated_date",
            th.DateType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "version",
            th.StringType,
            # description="Role granted to the user querying the account"
        ),
        th.Property(
            "type",
            th.StringType,
            # description="Role granted to the user querying the account"
        ),
        # th.Property(
        #     "devices",
        #     th.ObjectType,
        #     # description="Role granted to the user querying the account"
        # ),
        th.Property(
            "parent_id",
            th.IntegerType,
            # description="Role granted to the user querying the account"
        ),
        # th.Property(
        #     "children",
        #     th.ObjectType,
        #     # description="Role granted to the user querying the account"
        # ),
        # th.Property(
        #     "features",
        #     th.ObjectType,
        #     # description="Role granted to the user querying the account"
        # ),
        #  th.Property("source", th.ObjectType(
        #     th.Property("amount", th.NumberType),
        #     th.Property("currency", th.StringType),
        # )),       
    ).to_dict()

    # def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
    #     """Return a context dictionary for child streams."""
    #     return {
    #         "account_id": record["accountId"],
    #         "account_type": record["accountType"]
    #     }


class SubscriptionsStream(AppFiguresStream):
    name = "subscriptions"
    # parent_stream_type = AccountsStream
    # ignore_parent_replication_keys = True
    path = "/reports/subscriptions"
    primary_keys = ["date", "country", "product_id"]
    replication_key = "date"
    records_jsonpath = "$[*]"
    schema = th.PropertiesList(
        th.Property("product_id", th.IntegerType),
        th.Property("country", th.StringType),
        th.Property("date", th.DateType),
        th.Property("all_active_subscriptions", th.IntegerType),
        th.Property("active_subscriptions", th.IntegerType),
        th.Property("paying_subscriptions", th.IntegerType),
        th.Property("actual_revenue", th.NumberType),
        th.Property("mrr", th.NumberType),
        th.Property("gross_mrr", th.NumberType),
        th.Property("gross_revenue", th.NumberType),
        th.Property("activations", th.IntegerType),
        th.Property("cancelations", th.IntegerType),
        th.Property("churn", th.NumberType),
        th.Property("first_year_subscribers", th.IntegerType),
        th.Property("non_first_year_subscribers", th.IntegerType),
        th.Property("active_discounted_subscriptions", th.IntegerType),
        th.Property("active_trials", th.IntegerType),
        th.Property("new_trials", th.IntegerType),
        th.Property("cancelled_trials", th.IntegerType),
        th.Property("transitions_in", th.IntegerType),
        th.Property("transitions_out", th.IntegerType),
        th.Property("cancelled_subscriptions", th.IntegerType),
        th.Property("new_subscriptions", th.IntegerType),
        th.Property("trial_conversions", th.IntegerType),
        th.Property("reactivations", th.IntegerType),
        th.Property("renewals", th.IntegerType),
        th.Property("active_grace", th.IntegerType),
        th.Property("new_grace", th.IntegerType),
        th.Property("grace_drop_off", th.IntegerType),
        th.Property("grace_recovery", th.IntegerType),
        th.Property("new_trial_grace", th.IntegerType),
        th.Property("trial_grace_drop_off", th.IntegerType),
        th.Property("trial_grace_recovery", th.IntegerType),
        
        
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        # start_date = self.get_starting_timestamp(context) - datetime.timedelta(days=self.config.get("lookback_days"))
        # end_date = min(start_date + datetime.timedelta(days=1), today)
        params = {
            'startDate': self.config.get("start_date"),
            'endDate': self.config.get("end_date"),
            "group_by": "product,date,country",
            "granularity": "daily"
        }
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:

        data = response.json()
        records = []
        for product_id, product_data in data.items():
            for country, country_data in product_data.items():
                for date, date_data in country_data.items():
                    if not date_data:
                        continue
                    
                    row = { 
                        "product_id": product_id,
                        "country": country,
                        "date": date,
                    }
                    row.update(date_data)
                    records.append(row)

        yield from extract_jsonpath(self.records_jsonpath, input=records)

        # with open("af_subscription.json", "w") as f:
        #     f.write(json.dumps((response.json()), indent=4))



# class PublishersStream(AppFiguresStream):
#     name = "publishers"
#     parent_stream_type = AccountsStream
#     ignore_parent_replication_keys = True
#     path = "/advertisers/{account_id}/publishers"
#     primary_keys = ["id"]
#     replication_key = None
#     records_jsonpath = "$[*]"
#     next_page_token_jsonpath = None
#     schema = th.PropertiesList(
#         th.Property(
#             "id",
#             th.IntegerType,
#             description="The Publisher's ID"
#         ),
#         th.Property(
#             "name",
#             th.StringType,
#             description="Publisher name"
#         ),
#         th.Property(
#             "primaryRegion",
#             th.StringType,
#             description="Publisher primary region"
#         ),
#         th.Property(
#             "salesRegions",
#             th.ArrayType(th.StringType),
#             description="Additional sales regions of the publisher (if defined)"
#         ),
#         th.Property(
#             "primaryType",
#             th.StringType,
#             description="Primary promotion type of the publisher"
#         ),
#     ).to_dict()

#     def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
#         """Return a generator of row-type dictionary objects.
#         Each row emitted should be a dictionary of property names to their values.
#         """
#         if context["account_type"] != "advertiser":
#             self.logger.debug("Skipping account {account_id} publishers.".format(account_id=context["account_id"]))
#             return []
#         return super().get_records(context)

#     def validate_response(self, response: requests.Response) -> None:
#         if response.status_code == 104:
#             msg = (
#                 f"{response.status_code} Server Error: "
#                 f"{response.reason} for path: {self.path}"
#             )
#             raise RetriableAPIError(msg)
#         elif 400 <= response.status_code < 500:
#             msg = (
#                 f"{response.status_code} Client Error: "
#                 f"{response.reason} for path: {self.path}"
#             )
#             raise FatalAPIError(msg)
#         elif 500 <= response.status_code < 600:
#             msg = (
#                 f"{response.status_code} Server Error: "
#                 f"{response.reason} for path: {self.path}"
#             )
#             raise RetriableAPIError(msg)


# REPORT_TIMESTAMP_FORMAT = "%Y-%m-%d"

# class ReportByPublisherStream(AppFiguresStream):
#     name = "report_by_publisher"
#     parent_stream_type = AccountsStream
#     ignore_parent_replication_keys = True
#     path = "/{account_type}s/{account_id}/reports/publisher"
#     primary_keys = ["advertiserId", "publisherId", "transactionDate"]
#     replication_key = "transactionDate"
#     records_jsonpath = "$[*]"
#     next_page_token_jsonpath = None
#     schema = th.PropertiesList(
#         th.Property("advertiserId", th.IntegerType),
#         th.Property("advertiserName", th.StringType),
#         th.Property("publisherId", th.IntegerType),
#         th.Property("publisherName", th.StringType),
#         th.Property("transactionDate", th.DateTimeType),
#         th.Property("region", th.StringType),
#         th.Property("currency", th.StringType),
#         th.Property("impressions", th.NumberType),
#         th.Property("clicks", th.NumberType),
#         th.Property("pendingNo", th.NumberType),
#         th.Property("pendingValue", th.NumberType),
#         th.Property("pendingComm", th.NumberType),
#         th.Property("confirmedNo", th.NumberType),
#         th.Property("confirmedValue", th.NumberType),
#         th.Property("confirmedComm", th.NumberType),
#         th.Property("bonusNo", th.NumberType),
#         th.Property("bonusValue", th.NumberType),
#         th.Property("bonusComm", th.NumberType),
#         th.Property("totalNo", th.NumberType),
#         th.Property("totalValue", th.NumberType),
#         th.Property("totalComm", th.NumberType),
#         th.Property("declinedNo", th.NumberType),
#         th.Property("declinedValue", th.NumberType),
#         th.Property("declinedComm", th.NumberType),
#         th.Property("tags", th.ArrayType(th.StringType)),
#     ).to_dict()

#     def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
#         """Return a generator of row-type dictionary objects.
#         Each row emitted should be a dictionary of property names to their values.
#         """
#         if context["account_type"] != "advertiser":
#             self.logger.debug("Skipping account {account_id} publishers.".format(account_id=context["account_id"]))
#             return []

#         return super().get_records(context)

#     def get_url_params(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Optional[dict]:
#         if isinstance(next_page_token, datetime.datetime):
#             start_date = next_page_token
#         else:
#             start_date = self.get_starting_timestamp(context) - datetime.timedelta(days=self.config.get("lookback_days"))
#         today = datetime.datetime.now(tz=start_date.tzinfo)
#         end_date = min(start_date + datetime.timedelta(days=1), today)
#         params = {
#             'startDate': datetime.datetime.strftime(
#                 start_date.replace(hour=0, minute=0, second=0, microsecond=0),
#                 REPORT_TIMESTAMP_FORMAT
#             ),
#             'endDate': datetime.datetime.strftime(
#                 end_date.replace(hour=0, minute=0, second=0, microsecond=0),
#                 REPORT_TIMESTAMP_FORMAT
#             ),
#             'timezone': self.config.get("timezone"),
#             'dateType': 'transaction',
#             'accessToken': self.config.get("api_token")
#         }

#         self.logger.debug("URL params: %s", params)
        
#         return params

#     def get_next_page_token(
#         self, response: requests.Response, previous_token: Optional[Any]
#     ) -> Optional[Any]:
#         """Return a token for identifying next page or None if no more pages."""
#         if self.next_page_token_jsonpath:
#             all_matches = extract_jsonpath(
#                 self.next_page_token_jsonpath, response.json()
#             )
#             first_match = next(iter(all_matches), None)
#             next_page_token = first_match
#         elif response.headers.get("X-Next-Page", None):
#             next_page_token = response.headers.get("X-Next-Page", None)
#         else:
#             end_date = datetime.datetime.strptime(
#                 parse_qs(urlparse(response.request.url).query)['endDate'][0],
#                 REPORT_TIMESTAMP_FORMAT
#             )
#             if end_date.date() < datetime.date.today():
#                 next_page_token = end_date
#             else:
#                 next_page_token = None
#         return next_page_token

#     def parse_response(self, response: requests.Response) -> Iterable[dict]:
#         response_json = response.json()
#         start_date = datetime.datetime.strptime(
#             parse_qs(urlparse(response.request.url).query)['startDate'][0],
#             REPORT_TIMESTAMP_FORMAT
#         )
#         for row in response_json:
#             row["transactionDate"] = start_date
#         yield from extract_jsonpath(self.records_jsonpath, input=response_json)
