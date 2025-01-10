r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AnalyticsCollectionInfoByAccessedTime", "AnalyticsCollectionInfoByAccessedTimeSchema"]
__pdoc__ = {
    "AnalyticsCollectionInfoByAccessedTimeSchema.resource": False,
    "AnalyticsCollectionInfoByAccessedTimeSchema.opts": False,
    "AnalyticsCollectionInfoByAccessedTime": False,
}


class AnalyticsCollectionInfoByAccessedTimeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AnalyticsCollectionInfoByAccessedTime object"""

    bytes_used = fields.Nested("netapp_ontap.models.analytics_collection_info_by_accessed_time_bytes_used.AnalyticsCollectionInfoByAccessedTimeBytesUsedSchema", unknown=EXCLUDE, data_key="bytes_used")
    r""" The bytes_used field of the analytics_collection_info_by_accessed_time. """

    @property
    def resource(self):
        return AnalyticsCollectionInfoByAccessedTime

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


class AnalyticsCollectionInfoByAccessedTime(Resource):

    _schema = AnalyticsCollectionInfoByAccessedTimeSchema
