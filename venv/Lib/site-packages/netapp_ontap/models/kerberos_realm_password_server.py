r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["KerberosRealmPasswordServer", "KerberosRealmPasswordServerSchema"]
__pdoc__ = {
    "KerberosRealmPasswordServerSchema.resource": False,
    "KerberosRealmPasswordServerSchema.opts": False,
    "KerberosRealmPasswordServer": False,
}


class KerberosRealmPasswordServerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the KerberosRealmPasswordServer object"""

    address = fields.Str(data_key="address")
    r""" Password server IP address.

Example: 1.2.3.4 """

    port = Size(data_key="port")
    r""" Specifies the port number of password server. """

    @property
    def resource(self):
        return KerberosRealmPasswordServer

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


class KerberosRealmPasswordServer(Resource):

    _schema = KerberosRealmPasswordServerSchema
