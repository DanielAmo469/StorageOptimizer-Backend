r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsSvmClientExcludedVolumeReason", "TopMetricsSvmClientExcludedVolumeReasonSchema"]
__pdoc__ = {
    "TopMetricsSvmClientExcludedVolumeReasonSchema.resource": False,
    "TopMetricsSvmClientExcludedVolumeReasonSchema.opts": False,
    "TopMetricsSvmClientExcludedVolumeReason": False,
}


class TopMetricsSvmClientExcludedVolumeReasonSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsSvmClientExcludedVolumeReason object"""

    code = fields.Str(data_key="code")
    r""" Warning code indicating why the volume is not included in the SVM activity tracking REST API.

Example: 111411207 """

    message = fields.Str(data_key="message")
    r""" Details why the volume is not included in the SVM activity tracking REST API.

Example: The volume is offline. """

    @property
    def resource(self):
        return TopMetricsSvmClientExcludedVolumeReason

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


class TopMetricsSvmClientExcludedVolumeReason(Resource):

    _schema = TopMetricsSvmClientExcludedVolumeReasonSchema
