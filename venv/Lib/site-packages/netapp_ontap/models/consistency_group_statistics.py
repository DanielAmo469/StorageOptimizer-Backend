r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupStatistics", "ConsistencyGroupStatisticsSchema"]
__pdoc__ = {
    "ConsistencyGroupStatisticsSchema.resource": False,
    "ConsistencyGroupStatisticsSchema.opts": False,
    "ConsistencyGroupStatistics": False,
}


class ConsistencyGroupStatisticsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupStatistics object"""

    available_space = Size(data_key="available_space")
    r""" The total space available in the consistency group, in bytes.

Example: 4096 """

    iops_raw = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="iops_raw")
    r""" The iops_raw field of the consistency_group_statistics. """

    latency_raw = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="latency_raw")
    r""" The latency_raw field of the consistency_group_statistics. """

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

    throughput_raw = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="throughput_raw")
    r""" The throughput_raw field of the consistency_group_statistics. """

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" The timestamp of the performance data.

Example: 2017-01-25T11:20:13.000+0000 """

    used_space = Size(data_key="used_space")
    r""" The total used space in the consistency group, in bytes.

Example: 4096 """

    @property
    def resource(self):
        return ConsistencyGroupStatistics

    gettable_fields = [
        "available_space",
        "iops_raw.other",
        "iops_raw.read",
        "iops_raw.total",
        "iops_raw.write",
        "latency_raw.other",
        "latency_raw.read",
        "latency_raw.total",
        "latency_raw.write",
        "size",
        "status",
        "throughput_raw.other",
        "throughput_raw.read",
        "throughput_raw.total",
        "throughput_raw.write",
        "timestamp",
        "used_space",
    ]
    """available_space,iops_raw.other,iops_raw.read,iops_raw.total,iops_raw.write,latency_raw.other,latency_raw.read,latency_raw.total,latency_raw.write,size,status,throughput_raw.other,throughput_raw.read,throughput_raw.total,throughput_raw.write,timestamp,used_space,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ConsistencyGroupStatistics(Resource):

    _schema = ConsistencyGroupStatisticsSchema
