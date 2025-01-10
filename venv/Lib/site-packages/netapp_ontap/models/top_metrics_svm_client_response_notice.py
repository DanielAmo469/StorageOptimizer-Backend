r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsSvmClientResponseNotice", "TopMetricsSvmClientResponseNoticeSchema"]
__pdoc__ = {
    "TopMetricsSvmClientResponseNoticeSchema.resource": False,
    "TopMetricsSvmClientResponseNoticeSchema.opts": False,
    "TopMetricsSvmClientResponseNotice": False,
}


class TopMetricsSvmClientResponseNoticeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsSvmClientResponseNotice object"""

    code = fields.Str(data_key="code")
    r""" Warning code indicating why no records are returned.

Example: 111411207 """

    message = fields.Str(data_key="message")
    r""" Details why no records are returned.

Example: The volume is offline. """

    @property
    def resource(self):
        return TopMetricsSvmClientResponseNotice

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


class TopMetricsSvmClientResponseNotice(Resource):

    _schema = TopMetricsSvmClientResponseNoticeSchema
