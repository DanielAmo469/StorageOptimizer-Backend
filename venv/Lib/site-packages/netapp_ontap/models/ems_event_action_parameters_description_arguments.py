r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventActionParametersDescriptionArguments", "EmsEventActionParametersDescriptionArgumentsSchema"]
__pdoc__ = {
    "EmsEventActionParametersDescriptionArgumentsSchema.resource": False,
    "EmsEventActionParametersDescriptionArgumentsSchema.opts": False,
    "EmsEventActionParametersDescriptionArguments": False,
}


class EmsEventActionParametersDescriptionArgumentsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventActionParametersDescriptionArguments object"""

    code = fields.Str(data_key="code")
    r""" Argument code """

    message = fields.Str(data_key="message")
    r""" Message argument """

    @property
    def resource(self):
        return EmsEventActionParametersDescriptionArguments

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


class EmsEventActionParametersDescriptionArguments(Resource):

    _schema = EmsEventActionParametersDescriptionArgumentsSchema
