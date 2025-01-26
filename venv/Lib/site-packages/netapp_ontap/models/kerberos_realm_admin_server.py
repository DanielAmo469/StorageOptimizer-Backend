r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["KerberosRealmAdminServer", "KerberosRealmAdminServerSchema"]
__pdoc__ = {
    "KerberosRealmAdminServerSchema.resource": False,
    "KerberosRealmAdminServerSchema.opts": False,
    "KerberosRealmAdminServer": False,
}


class KerberosRealmAdminServerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the KerberosRealmAdminServer object"""

    address = fields.Str(data_key="address")
    r""" Admin server IP address.

Example: 1.2.3.4 """

    port = Size(data_key="port")
    r""" Specifies the port number of admin server. """

    @property
    def resource(self):
        return KerberosRealmAdminServer

    gettable_fields = [
        "address",
        "port",
    ]
    """address,port,"""

    patchable_fields = [
        "address",
        "port",
    ]
    """address,port,"""

    postable_fields = [
        "address",
        "port",
    ]
    """address,port,"""


class KerberosRealmAdminServer(Resource):

    _schema = KerberosRealmAdminServerSchema
