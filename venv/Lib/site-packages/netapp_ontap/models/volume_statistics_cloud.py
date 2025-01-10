r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeStatisticsCloud", "VolumeStatisticsCloudSchema"]
__pdoc__ = {
    "VolumeStatisticsCloudSchema.resource": False,
    "VolumeStatisticsCloudSchema.opts": False,
    "VolumeStatisticsCloud": False,
}


class VolumeStatisticsCloudSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeStatisticsCloud object"""

    iops_raw = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="iops_raw")
    r""" The iops_raw field of the volume_statistics_cloud. """

    latency_raw = fields.Nested("netapp_ontap.models.performance_metric_io_type.PerformanceMetricIoTypeSchema", unknown=EXCLUDE, data_key="latency_raw")
    r""" The latency_raw field of the volume_statistics_cloud. """

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

    timestamp = ImpreciseDateTime(data_key="timestamp")
    r""" The timestamp of the performance data.

Example: 2017-01-25T11:20:13.000+0000 """

    @property
    def resource(self):
        return VolumeStatisticsCloud

    gettable_fields = [
        "iops_raw.other",
        "iops_raw.read",
        "iops_raw.total",
        "iops_raw.write",
        "latency_raw.other",
        "latency_raw.read",
        "latency_raw.total",
        "latency_raw.write",
        "status",
        "timestamp",
    ]
    """iops_raw.other,iops_raw.read,iops_raw.total,iops_raw.write,latency_raw.other,latency_raw.read,latency_raw.total,latency_raw.write,status,timestamp,"""

    patchable_fields = [
        "iops_raw.other",
        "iops_raw.read",
        "iops_raw.total",
        "iops_raw.write",
        "latency_raw.other",
        "latency_raw.read",
        "latency_raw.total",
        "latency_raw.write",
    ]
    """iops_raw.other,iops_raw.read,iops_raw.total,iops_raw.write,latency_raw.other,latency_raw.read,latency_raw.total,latency_raw.write,"""

    postable_fields = [
        "iops_raw.other",
        "iops_raw.read",
        "iops_raw.total",
        "iops_raw.write",
        "latency_raw.other",
        "latency_raw.read",
        "latency_raw.total",
        "latency_raw.write",
    ]
    """iops_raw.other,iops_raw.read,iops_raw.total,iops_raw.write,latency_raw.other,latency_raw.read,latency_raw.total,latency_raw.write,"""


class VolumeStatisticsCloud(Resource):

    _schema = VolumeStatisticsCloudSchema
