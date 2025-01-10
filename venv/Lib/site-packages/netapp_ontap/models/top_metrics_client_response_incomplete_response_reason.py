r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsClientResponseIncompleteResponseReason", "TopMetricsClientResponseIncompleteResponseReasonSchema"]
__pdoc__ = {
    "TopMetricsClientResponseIncompleteResponseReasonSchema.resource": False,
    "TopMetricsClientResponseIncompleteResponseReasonSchema.opts": False,
    "TopMetricsClientResponseIncompleteResponseReason": False,
}


class TopMetricsClientResponseIncompleteResponseReasonSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsClientResponseIncompleteResponseReason object"""

    code = fields.Str(data_key="code")
    r""" Warning code indicating why partial data was reported.

Example: 111411207 """

    message = fields.Str(data_key="message")
    r""" A message describing the reason for partial data.

Example: Partial data has been returned for this metric report. Reason: The activity tracking report for this volume is not available because the system is busy collecting tracking data. """

    @property
    def resource(self):
        return TopMetricsClientResponseIncompleteResponseReason

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


class TopMetricsClientResponseIncompleteResponseReason(Resource):

    _schema = TopMetricsClientResponseIncompleteResponseReasonSchema
