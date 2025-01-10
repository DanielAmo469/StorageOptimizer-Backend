r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReason", "ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReasonSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReasonSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReasonSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReason": False,
}


class ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReasonSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReason object"""

    code = fields.Str(data_key="code")
    r""" If volume activity tracking is not supported on the volume, this field provides an appropriate error code.

Example: 124518405 """

    message = fields.Str(data_key="message")
    r""" If volume activity tracking is not supported on the volume, this field provides an error message detailing why this is the case.

Example: Volume activity tracking cannot be enabled on volumes that contain LUNs. """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReason

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


class ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReason(Resource):

    _schema = ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReasonSchema
