r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LdapSchemaAccount", "LdapSchemaAccountSchema"]
__pdoc__ = {
    "LdapSchemaAccountSchema.resource": False,
    "LdapSchemaAccountSchema.opts": False,
    "LdapSchemaAccount": False,
}


class LdapSchemaAccountSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LdapSchemaAccount object"""

    unix = fields.Str(data_key="unix")
    r""" Attribute name used to retrieve UNIX account information.

Example: windowsAccount """

    windows = fields.Str(data_key="windows")
    r""" Attribute name used to retrieve Windows account information for a UNIX user account.

Example: windowsAccount """

    @property
    def resource(self):
        return LdapSchemaAccount

    gettable_fields = [
        "unix",
        "windows",
    ]
    """unix,windows,"""

    patchable_fields = [
        "unix",
        "windows",
    ]
    """unix,windows,"""

    postable_fields = [
        "unix",
        "windows",
    ]
    """unix,windows,"""


class LdapSchemaAccount(Resource):

    _schema = LdapSchemaAccountSchema
