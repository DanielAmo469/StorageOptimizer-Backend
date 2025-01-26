r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsOpenFileSession", "CifsOpenFileSessionSchema"]
__pdoc__ = {
    "CifsOpenFileSessionSchema.resource": False,
    "CifsOpenFileSessionSchema.opts": False,
    "CifsOpenFileSession": False,
}


class CifsOpenFileSessionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsOpenFileSession object"""

    identifier = Size(data_key="identifier")
    r""" Session under which file is opened.

Example: 8966666858094657537 """

    @property
    def resource(self):
        return CifsOpenFileSession

    gettable_fields = [
        "identifier",
    ]
    """identifier,"""

    patchable_fields = [
        "identifier",
    ]
    """identifier,"""

    postable_fields = [
        "identifier",
    ]
    """identifier,"""


class CifsOpenFileSession(Resource):

    _schema = CifsOpenFileSessionSchema
