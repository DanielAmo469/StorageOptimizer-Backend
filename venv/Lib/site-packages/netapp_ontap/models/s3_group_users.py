r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["S3GroupUsers", "S3GroupUsersSchema"]
__pdoc__ = {
    "S3GroupUsersSchema.resource": False,
    "S3GroupUsersSchema.opts": False,
    "S3GroupUsers": False,
}


class S3GroupUsersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the S3GroupUsers object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the s3_group_users. """

    name = fields.Str(data_key="name")
    r""" Specifies the name of the user. A user name length can range from 1 to 64 characters and can only contain the following combination of characters 0-9, A-Z, a-z, "_", "+", "=", ",", ".","@", and "-".

Example: user-1 """

    @property
    def resource(self):
        return S3GroupUsers

    gettable_fields = [
        "links",
        "name",
    ]
    """links,name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class S3GroupUsers(Resource):

    _schema = S3GroupUsersSchema
