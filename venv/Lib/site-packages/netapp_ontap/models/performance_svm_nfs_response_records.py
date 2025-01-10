r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["PerformanceSvmNfsResponseRecords", "PerformanceSvmNfsResponseRecordsSchema"]
__pdoc__ = {
    "PerformanceSvmNfsResponseRecordsSchema.resource": False,
    "PerformanceSvmNfsResponseRecordsSchema.opts": False,
    "PerformanceSvmNfsResponseRecords": False,
}


class PerformanceSvmNfsResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the PerformanceSvmNfsResponseRecords object"""

    v3 = fields.Nested("netapp_ontap.models.performance_metric_svm.PerformanceMetricSvmSchema", unknown=EXCLUDE, data_key="v3")
    r""" The v3 field of the performance_svm_nfs_response_records. """

    v4 = fields.Nested("netapp_ontap.models.performance_metric_svm.PerformanceMetricSvmSchema", unknown=EXCLUDE, data_key="v4")
    r""" The v4 field of the performance_svm_nfs_response_records. """

    v41 = fields.Nested("netapp_ontap.models.performance_metric_svm.PerformanceMetricSvmSchema", unknown=EXCLUDE, data_key="v41")
    r""" The v41 field of the performance_svm_nfs_response_records. """

    @property
    def resource(self):
        return PerformanceSvmNfsResponseRecords

    gettable_fields = [
        "v3.links",
        "v3.duration",
        "v3.iops",
        "v3.latency",
        "v3.status",
        "v3.throughput",
        "v3.timestamp",
        "v4.links",
        "v4.duration",
        "v4.iops",
        "v4.latency",
        "v4.status",
        "v4.throughput",
        "v4.timestamp",
        "v41.links",
        "v41.duration",
        "v41.iops",
        "v41.latency",
        "v41.status",
        "v41.throughput",
        "v41.timestamp",
    ]
    """v3.links,v3.duration,v3.iops,v3.latency,v3.status,v3.throughput,v3.timestamp,v4.links,v4.duration,v4.iops,v4.latency,v4.status,v4.throughput,v4.timestamp,v41.links,v41.duration,v41.iops,v41.latency,v41.status,v41.throughput,v41.timestamp,"""

    patchable_fields = [
        "v3.iops",
        "v3.latency",
        "v3.throughput",
        "v4.iops",
        "v4.latency",
        "v4.throughput",
        "v41.iops",
        "v41.latency",
        "v41.throughput",
    ]
    """v3.iops,v3.latency,v3.throughput,v4.iops,v4.latency,v4.throughput,v41.iops,v41.latency,v41.throughput,"""

    postable_fields = [
        "v3.iops",
        "v3.latency",
        "v3.throughput",
        "v4.iops",
        "v4.latency",
        "v4.throughput",
        "v41.iops",
        "v41.latency",
        "v41.throughput",
    ]
    """v3.iops,v3.latency,v3.throughput,v4.iops,v4.latency,v4.throughput,v41.iops,v41.latency,v41.throughput,"""


class PerformanceSvmNfsResponseRecords(Resource):

    _schema = PerformanceSvmNfsResponseRecordsSchema
