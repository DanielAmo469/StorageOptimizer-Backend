r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["UnixGroupUsersUnixGroup", "UnixGroupUsersUnixGroupSchema"]
__pdoc__ = {
    "UnixGroupUsersUnixGroupSchema.resource": False,
    "UnixGroupUsersUnixGroupSchema.opts": False,
    "UnixGroupUsersUnixGroup": False,
}


class UnixGroupUsersUnixGroupSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the UnixGroupUsersUnixGroup object"""

    name = fields.Str(data_key="name")
    r""" UNIX group name. """

    @property
    def resource(self):
        return UnixGroupUsersUnixGroup

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class UnixGroupUsersUnixGroup(Resource):

    _schema = UnixGroupUsersUnixGroupSchema
