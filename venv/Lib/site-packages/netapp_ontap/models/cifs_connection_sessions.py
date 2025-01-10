r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsConnectionSessions", "CifsConnectionSessionsSchema"]
__pdoc__ = {
    "CifsConnectionSessionsSchema.resource": False,
    "CifsConnectionSessionsSchema.opts": False,
    "CifsConnectionSessions": False,
}


class CifsConnectionSessionsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsConnectionSessions object"""

    identifier = Size(data_key="identifier")
    r""" A unique 64-bit unsigned number represented as string used to represent each SMB session's identifier.


Example: 4622663542519103507 """

    @property
    def resource(self):
        return CifsConnectionSessions

    gettable_fields = [
        "identifier",
    ]
    """identifier,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class CifsConnectionSessions(Resource):

    _schema = CifsConnectionSessionsSchema
