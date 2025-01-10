r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsSvmUserThroughput", "TopMetricsSvmUserThroughputSchema"]
__pdoc__ = {
    "TopMetricsSvmUserThroughputSchema.resource": False,
    "TopMetricsSvmUserThroughputSchema.opts": False,
    "TopMetricsSvmUserThroughput": False,
}


class TopMetricsSvmUserThroughputSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsSvmUserThroughput object"""

    error = fields.Nested("netapp_ontap.models.top_metric_value_error_bounds.TopMetricValueErrorBoundsSchema", unknown=EXCLUDE, data_key="error")
    r""" The error field of the top_metrics_svm_user_throughput. """

    read = Size(data_key="read")
    r""" Average number of read bytes received per second.

Example: 10 """

    write = Size(data_key="write")
    r""" Average number of write bytes received per second.

Example: 7 """

    @property
    def resource(self):
        return TopMetricsSvmUserThroughput

    gettable_fields = [
        "error",
        "read",
        "write",
    ]
    """error,read,write,"""

    patchable_fields = [
        "error",
    ]
    """error,"""

    postable_fields = [
        "error",
    ]
    """error,"""


class TopMetricsSvmUserThroughput(Resource):

    _schema = TopMetricsSvmUserThroughputSchema
