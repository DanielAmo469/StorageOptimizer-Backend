r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsClientResponseNotice", "TopMetricsClientResponseNoticeSchema"]
__pdoc__ = {
    "TopMetricsClientResponseNoticeSchema.resource": False,
    "TopMetricsClientResponseNoticeSchema.opts": False,
    "TopMetricsClientResponseNotice": False,
}


class TopMetricsClientResponseNoticeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsClientResponseNotice object"""

    code = fields.Str(data_key="code")
    r""" Warning code indicating why no records are returned.

Example: 111411207 """

    message = fields.Str(data_key="message")
    r""" Details why no records are returned.

Example: No read/write traffic on volume """

    @property
    def resource(self):
        return TopMetricsClientResponseNotice

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


class TopMetricsClientResponseNotice(Resource):

    _schema = TopMetricsClientResponseNoticeSchema
