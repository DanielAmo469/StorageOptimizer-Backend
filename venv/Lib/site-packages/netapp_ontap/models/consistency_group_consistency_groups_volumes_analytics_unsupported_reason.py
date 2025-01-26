r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReason", "ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReasonSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReasonSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReasonSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReason": False,
}


class ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReasonSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReason object"""

    code = fields.Str(data_key="code")
    r""" If file system analytics is not supported on the volume, this field provides the error code explaining why.

Example: 111411207 """

    message = fields.Str(data_key="message")
    r""" If file system analytics is not supported on the volume, this field provides the error message explaining why.

Example: File system analytics cannot be enabled on volumes that contain LUNs. """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReason

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReason(Resource):

    _schema = ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReasonSchema
