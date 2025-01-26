r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["KerberosRealmAdServer", "KerberosRealmAdServerSchema"]
__pdoc__ = {
    "KerberosRealmAdServerSchema.resource": False,
    "KerberosRealmAdServerSchema.opts": False,
    "KerberosRealmAdServer": False,
}


class KerberosRealmAdServerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the KerberosRealmAdServer object"""

    address = fields.Str(data_key="address")
    r""" Active Directory server IP address

Example: 1.2.3.4 """

    name = fields.Str(data_key="name")
    r""" Active Directory server name """

    @property
    def resource(self):
        return KerberosRealmAdServer

    gettable_fields = [
        "address",
        "name",
    ]
    """address,name,"""

    patchable_fields = [
        "address",
        "name",
    ]
    """address,name,"""

    postable_fields = [
        "address",
        "name",
    ]
    """address,name,"""


class KerberosRealmAdServer(Resource):

    _schema = KerberosRealmAdServerSchema
