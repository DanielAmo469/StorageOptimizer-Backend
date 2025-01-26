r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AnalyticsCollectionInfo", "AnalyticsCollectionInfoSchema"]
__pdoc__ = {
    "AnalyticsCollectionInfoSchema.resource": False,
    "AnalyticsCollectionInfoSchema.opts": False,
    "AnalyticsCollectionInfo": False,
}


class AnalyticsCollectionInfoSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AnalyticsCollectionInfo object"""

    by_accessed_time = fields.Nested("netapp_ontap.models.analytics_collection_info_by_accessed_time.AnalyticsCollectionInfoByAccessedTimeSchema", unknown=EXCLUDE, data_key="by_accessed_time")
    r""" The by_accessed_time field of the analytics_collection_info. """

    by_modified_time = fields.Nested("netapp_ontap.models.analytics_collection_info_by_modified_time.AnalyticsCollectionInfoByModifiedTimeSchema", unknown=EXCLUDE, data_key="by_modified_time")
    r""" The by_modified_time field of the analytics_collection_info. """

    @property
    def resource(self):
        return AnalyticsCollectionInfo

    gettable_fields = [
        "by_accessed_time",
        "by_modified_time",
    ]
    """by_accessed_time,by_modified_time,"""

    patchable_fields = [
        "by_accessed_time",
        "by_modified_time",
    ]
    """by_accessed_time,by_modified_time,"""

    postable_fields = [
        "by_accessed_time",
        "by_modified_time",
    ]
    """by_accessed_time,by_modified_time,"""


class AnalyticsCollectionInfo(Resource):

    _schema = AnalyticsCollectionInfoSchema
