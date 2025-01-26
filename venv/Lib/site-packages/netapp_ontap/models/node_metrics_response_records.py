r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeMetricsResponseRecords", "NodeMetricsResponseRecordsSchema"]
__pdoc__ = {
    "NodeMetricsResponseRecordsSchema.resource": False,
    "NodeMetricsResponseRecordsSchema.opts": False,
    "NodeMetricsResponseRecords": False,
}


class NodeMetricsResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeMetricsResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the node_metrics_response_records. """

    duration = fields.Str(data_key="duration")
    r""" The duration over which this sample is calculated. The time durations are represented in the ISO-8601 standard format. Samples can be calculated over the following durations:


Valid choices:

* PT15S
* PT5M
* PT30M
* PT2H
* P1D """

    processor_utilization = Size(data_key="processor_utilization")
    r""" Average CPU Utilization for the node

Example: 13 """

    status = fields.Str(data_key="status")
    r""" Errors associated with the sample. For example, if the aggregation of data over multiple nodes fails, then any partial errors might return "ok" on success or "error" on an internal uncategorized failure. Whenever a sample collection is missed but done at a later time, it is back filled to the previous 15 second timestamp and tagged with "backfilled_data". "inconsistent_delta_time" is encountered when the time between two collections is not the same for all nodes. Therefore, the aggregated value might be over or under inflated. "Negative_delta" is returned when an expected monotonically increasing value has decreased in value. "inconsistent_old_data" is returned when one or more nodes do not have the latest data.

Valid choices:

* ok
* error
* partial_no_data
* partial_no_uuid
* partial_no_response
* partial_other_error
* negative_delta
* backfilled_data
* inconsistent_delta_time
* inconsistent_old_data """

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" The timestamp of the performance data.

Example: 2017-01-25T11:20:13.000+0000 """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the node_metrics_response_records.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return NodeMetricsResponseRecords

    gettable_fields = [
        "links",
        "duration",
        "processor_utilization",
        "status",
        "timestamp",
        "uuid",
    ]
    """links,duration,processor_utilization,status,timestamp,uuid,"""

    patchable_fields = [
        "duration",
        "processor_utilization",
        "status",
        "timestamp",
        "uuid",
    ]
    """duration,processor_utilization,status,timestamp,uuid,"""

    postable_fields = [
        "duration",
        "processor_utilization",
        "status",
        "timestamp",
        "uuid",
    ]
    """duration,processor_utilization,status,timestamp,uuid,"""


class NodeMetricsResponseRecords(Resource):

    _schema = NodeMetricsResponseRecordsSchema
