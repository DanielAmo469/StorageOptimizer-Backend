r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["PerformanceMetricIoTypeRwt", "PerformanceMetricIoTypeRwtSchema"]
__pdoc__ = {
    "PerformanceMetricIoTypeRwtSchema.resource": False,
    "PerformanceMetricIoTypeRwtSchema.opts": False,
    "PerformanceMetricIoTypeRwt": False,
}


class PerformanceMetricIoTypeRwtSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the PerformanceMetricIoTypeRwt object"""

    read = Size(data_key="read")
    r""" Performance metric for read I/O operations.

Example: 200 """

    total = Size(data_key="total")
    r""" Performance metric aggregated over all types of I/O operations.

Example: 1000 """

    write = Size(data_key="write")
    r""" Peformance metric for write I/O operations.

Example: 100 """

    @property
    def resource(self):
        return PerformanceMetricIoTypeRwt

    gettable_fields = [
        "read",
        "total",
        "write",
    ]
    """read,total,write,"""

    patchable_fields = [
        "read",
        "total",
        "write",
    ]
    """read,total,write,"""

    postable_fields = [
        "read",
        "total",
        "write",
    ]
    """read,total,write,"""


class PerformanceMetricIoTypeRwt(Resource):

    _schema = PerformanceMetricIoTypeRwtSchema
