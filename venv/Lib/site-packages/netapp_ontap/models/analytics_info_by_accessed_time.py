r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AnalyticsInfoByAccessedTime", "AnalyticsInfoByAccessedTimeSchema"]
__pdoc__ = {
    "AnalyticsInfoByAccessedTimeSchema.resource": False,
    "AnalyticsInfoByAccessedTimeSchema.opts": False,
    "AnalyticsInfoByAccessedTime": False,
}


class AnalyticsInfoByAccessedTimeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AnalyticsInfoByAccessedTime object"""

    bytes_used = fields.Nested("netapp_ontap.models.analytics_histogram_by_time.AnalyticsHistogramByTimeSchema", unknown=EXCLUDE, data_key="bytes_used")
    r""" The bytes_used field of the analytics_info_by_accessed_time. """

    @property
    def resource(self):
        return AnalyticsInfoByAccessedTime

    gettable_fields = [
        "bytes_used",
    ]
    """bytes_used,"""

    patchable_fields = [
        "bytes_used",
    ]
    """bytes_used,"""

    postable_fields = [
        "bytes_used",
    ]
    """bytes_used,"""


class AnalyticsInfoByAccessedTime(Resource):

    _schema = AnalyticsInfoByAccessedTimeSchema
