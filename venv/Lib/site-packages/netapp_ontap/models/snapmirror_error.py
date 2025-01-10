r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapmirrorError", "SnapmirrorErrorSchema"]
__pdoc__ = {
    "SnapmirrorErrorSchema.resource": False,
    "SnapmirrorErrorSchema.opts": False,
    "SnapmirrorError": False,
}


class SnapmirrorErrorSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapmirrorError object"""

    code = fields.Str(data_key="code")
    r""" Error code """

    message = fields.Str(data_key="message")
    r""" Error message """

    parameters = fields.List(fields.Str, data_key="parameters")
    r""" Parameters for the error message """

    @property
    def resource(self):
        return SnapmirrorError

    gettable_fields = [
        "code",
        "message",
        "parameters",
    ]
    """code,message,parameters,"""

    patchable_fields = [
        "code",
        "message",
        "parameters",
    ]
    """code,message,parameters,"""

    postable_fields = [
        "code",
        "message",
        "parameters",
    ]
    """code,message,parameters,"""


class SnapmirrorError(Resource):

    _schema = SnapmirrorErrorSchema
