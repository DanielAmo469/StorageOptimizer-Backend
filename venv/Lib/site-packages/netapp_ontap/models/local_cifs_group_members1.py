r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LocalCifsGroupMembers1", "LocalCifsGroupMembers1Schema"]
__pdoc__ = {
    "LocalCifsGroupMembers1Schema.resource": False,
    "LocalCifsGroupMembers1Schema.opts": False,
    "LocalCifsGroupMembers1": False,
}


class LocalCifsGroupMembers1Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LocalCifsGroupMembers1 object"""

    name = fields.Str(data_key="name")
    r""" Local user, Active Directory user, or Active Directory group which is a member of the specified local group. """

    @property
    def resource(self):
        return LocalCifsGroupMembers1

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "name",
    ]
    """name,"""


class LocalCifsGroupMembers1(Resource):

    _schema = LocalCifsGroupMembers1Schema
