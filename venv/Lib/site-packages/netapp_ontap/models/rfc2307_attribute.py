r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["Rfc2307Attribute", "Rfc2307AttributeSchema"]
__pdoc__ = {
    "Rfc2307AttributeSchema.resource": False,
    "Rfc2307AttributeSchema.opts": False,
    "Rfc2307Attribute": False,
}


class Rfc2307AttributeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Rfc2307Attribute object"""

    gecos = fields.Str(data_key="gecos")
    r""" RFC 2307 gecos attribute.

Example: name """

    gid_number = fields.Str(data_key="gid_number")
    r""" RFC 2307 gidNumber attribute.

Example: msSFU30GidNumber """

    home_directory = fields.Str(data_key="home_directory")
    r""" RFC 2307 homeDirectory attribute.

Example: msSFU30HomeDirectory """

    login_shell = fields.Str(data_key="login_shell")
    r""" RFC 2307 loginShell attribute.

Example: msSFU30LoginShell """

    uid = fields.Str(data_key="uid")
    r""" RFC 1274 userid attribute used by RFC 2307 as UID.

Example: sAMAccountName """

    uid_number = fields.Str(data_key="uid_number")
    r""" RFC 2307 uidNumber attribute.

Example: msSFU30UidNumber """

    user_password = fields.Str(data_key="user_password")
    r""" RFC 2256 userPassword attribute used by RFC 2307.

Example: msSFU30Password """

    @property
    def resource(self):
        return Rfc2307Attribute

    gettable_fields = [
        "gecos",
        "gid_number",
        "home_directory",
        "login_shell",
        "uid",
        "uid_number",
        "user_password",
    ]
    """gecos,gid_number,home_directory,login_shell,uid,uid_number,user_password,"""

    patchable_fields = [
        "gecos",
        "gid_number",
        "home_directory",
        "login_shell",
        "uid",
        "uid_number",
        "user_password",
    ]
    """gecos,gid_number,home_directory,login_shell,uid,uid_number,user_password,"""

    postable_fields = [
        "gecos",
        "gid_number",
        "home_directory",
        "login_shell",
        "uid",
        "uid_number",
        "user_password",
    ]
    """gecos,gid_number,home_directory,login_shell,uid,uid_number,user_password,"""


class Rfc2307Attribute(Resource):

    _schema = Rfc2307AttributeSchema
