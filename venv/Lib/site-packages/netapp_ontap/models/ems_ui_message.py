r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsUiMessage", "EmsUiMessageSchema"]
__pdoc__ = {
    "EmsUiMessageSchema.resource": False,
    "EmsUiMessageSchema.opts": False,
    "EmsUiMessage": False,
}


class EmsUiMessageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsUiMessage object"""

    arguments = fields.List(fields.Nested("netapp_ontap.models.ems_event_action_title_arguments.EmsEventActionTitleArgumentsSchema", unknown=EXCLUDE), data_key="arguments")
    r""" Message arguments """

    code = fields.Str(data_key="code")
    r""" Unique message code.

Example: 4 """

    message = fields.Str(data_key="message")
    r""" User message.

Example: entry doesn't exist """

    @property
    def resource(self):
        return EmsUiMessage

    gettable_fields = [
        "arguments",
        "code",
        "message",
    ]
    """arguments,code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class EmsUiMessage(Resource):

    _schema = EmsUiMessageSchema
