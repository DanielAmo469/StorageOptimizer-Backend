r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AggregateWarningAction", "AggregateWarningActionSchema"]
__pdoc__ = {
    "AggregateWarningActionSchema.resource": False,
    "AggregateWarningActionSchema.opts": False,
    "AggregateWarningAction": False,
}


class AggregateWarningActionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AggregateWarningAction object"""

    arguments = fields.List(fields.Str, data_key="arguments")
    r""" Arguments present in the specified action message. """

    code = Size(data_key="code")
    r""" Corrective action code of the specified action. """

    message = fields.Str(data_key="message")
    r""" Specifies the corrective action to be taken to resolve the issue. """

    @property
    def resource(self):
        return AggregateWarningAction

    gettable_fields = [
        "arguments",
        "code",
        "message",
    ]
    """arguments,code,message,"""

    patchable_fields = [
        "arguments",
        "code",
        "message",
    ]
    """arguments,code,message,"""

    postable_fields = [
        "arguments",
        "code",
        "message",
    ]
    """arguments,code,message,"""


class AggregateWarningAction(Resource):

    _schema = AggregateWarningActionSchema
