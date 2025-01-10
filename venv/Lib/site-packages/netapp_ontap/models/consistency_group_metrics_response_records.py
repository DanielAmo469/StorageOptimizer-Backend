r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupMetricsResponseRecords", "ConsistencyGroupMetricsResponseRecordsSchema"]
__pdoc__ = {
    "ConsistencyGroupMetricsResponseRecordsSchema.resource": False,
    "ConsistencyGroupMetricsResponseRecordsSchema.opts": False,
    "ConsistencyGroupMetricsResponseRecords": False,
}


class ConsistencyGroupMetricsResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupMetricsResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_metrics_response_records. """

    available_space = Size(data_key="available_space")
    r""" The total space available in the consistency group, in bytes.

Example: 4096 """

    duration = fields.Str(data_key="duration")
    r""" The duration over which this sample is calculated. The time durations are represented in the ISO-8601 standard format. Samples can be calculated over the following durations:


Valid choices:

* PT15S
* PT4M
* PT30M
* PT2H
* P1D
* PT5M """

    iops = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="iops")
    r""" The iops field of the consistency_group_metrics_response_records. """

    latency = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="latency")
    r""" The latency field of the consistency_group_metrics_response_records. """

    size = Size(data_key="size")
    r""" The total size of the consistency group, in bytes.

Example: 4096 """

    status = fields.Str(data_key="status")
    r""" Errors associated with the sample. For example, if the aggregation of data over multiple nodes fails, then any partial errors might return "ok" on success or "error" on an internal uncategorized failure. Whenever a sample collection is missed but done at a later time, it is back filled to the previous 15 second timestamp and tagged with "backfilled_data". "Inconsistent_ delta_time" is encountered when the time between two collections is not the same for all nodes. Therefore, the aggregated value might be over or under inflated. "Negative_delta" is returned when an expected monotonically increasing value has decreased in value. "Inconsistent_old_data" is returned when one or more nodes do not have the latest data.

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

    throughput = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="throughput")
    r""" The throughput field of the consistency_group_metrics_response_records. """

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" The timestamp of the performance and capacity data.

Example: 2017-01-25T11:20:13.000+0000 """

    used_space = Size(data_key="used_space")
    r""" The total space used in the consistency group, in bytes.

Example: 4096 """

    @property
    def resource(self):
        return ConsistencyGroupMetricsResponseRecords

    gettable_fields = [
        "links",
        "available_space",
        "duration",
        "iops.other",
        "iops.read",
        "iops.total",
        "iops.write",
        "latency.other",
        "latency.read",
        "latency.total",
        "latency.write",
        "size",
        "status",
        "throughput.other",
        "throughput.read",
        "throughput.total",
        "throughput.write",
        "timestamp",
        "used_space",
    ]
    """links,available_space,duration,iops.other,iops.read,iops.total,iops.write,latency.other,latency.read,latency.total,latency.write,size,status,throughput.other,throughput.read,throughput.total,throughput.write,timestamp,used_space,"""

    patchable_fields = [
        "iops.other",
        "iops.read",
        "iops.total",
        "iops.write",
        "latency.other",
        "latency.read",
        "latency.total",
        "latency.write",
        "throughput.other",
        "throughput.read",
        "throughput.total",
        "throughput.write",
    ]
    """iops.other,iops.read,iops.total,iops.write,latency.other,latency.read,latency.total,latency.write,throughput.other,throughput.read,throughput.total,throughput.write,"""

    postable_fields = [
        "iops.other",
        "iops.read",
        "iops.total",
        "iops.write",
        "latency.other",
        "latency.read",
        "latency.total",
        "latency.write",
        "throughput.other",
        "throughput.read",
        "throughput.total",
        "throughput.write",
    ]
    """iops.other,iops.read,iops.total,iops.write,latency.other,latency.read,latency.total,latency.write,throughput.other,throughput.read,throughput.total,throughput.write,"""


class ConsistencyGroupMetricsResponseRecords(Resource):

    _schema = ConsistencyGroupMetricsResponseRecordsSchema
