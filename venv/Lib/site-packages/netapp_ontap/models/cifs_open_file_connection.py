r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsOpenFileConnection", "CifsOpenFileConnectionSchema"]
__pdoc__ = {
    "CifsOpenFileConnectionSchema.resource": False,
    "CifsOpenFileConnectionSchema.opts": False,
    "CifsOpenFileConnection": False,
}


class CifsOpenFileConnectionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsOpenFileConnection object"""

    count = Size(data_key="count")
    r""" The number of CIFS connections associated with the CIFS session.

Example: 3 """

    identifier = Size(data_key="identifier")
    r""" The connection that is used to open the file.

Example: 356756 """

    @property
    def resource(self):
        return CifsOpenFileConnection

    gettable_fields = [
        "count",
        "identifier",
    ]
    """count,identifier,"""

    patchable_fields = [
        "identifier",
    ]
    """identifier,"""

    postable_fields = [
        "identifier",
    ]
    """identifier,"""


class CifsOpenFileConnection(Resource):

    _schema = CifsOpenFileConnectionSchema
