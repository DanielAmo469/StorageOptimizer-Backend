r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["UniformResourceIdentifier", "UniformResourceIdentifierSchema"]
__pdoc__ = {
    "UniformResourceIdentifierSchema.resource": False,
    "UniformResourceIdentifierSchema.opts": False,
    "UniformResourceIdentifier": False,
}


class UniformResourceIdentifierSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the UniformResourceIdentifier object"""

    password = fields.Str(data_key="password")
    r""" Password of the specified URI. """

    path = fields.Str(data_key="path")
    r""" URI from which to load the input file containing the CIFS local users and groups. The file must be encrypted using the 7zip utility. URI can be FTP or HTTP.

Example: http://web.sample.com/web1/file1.7z """

    username = fields.Str(data_key="username")
    r""" Username of the specified URI.

Example: user1 """

    @property
    def resource(self):
        return UniformResourceIdentifier

    gettable_fields = [
        "path",
    ]
    """path,"""

    patchable_fields = [
        "password",
        "path",
        "username",
    ]
    """password,path,username,"""

    postable_fields = [
        "password",
        "path",
        "username",
    ]
    """password,path,username,"""


class UniformResourceIdentifier(Resource):

    _schema = UniformResourceIdentifierSchema
