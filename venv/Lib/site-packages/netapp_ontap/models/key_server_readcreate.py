r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["KeyServerReadcreate", "KeyServerReadcreateSchema"]
__pdoc__ = {
    "KeyServerReadcreateSchema.resource": False,
    "KeyServerReadcreateSchema.opts": False,
    "KeyServerReadcreate": False,
}


class KeyServerReadcreateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the KeyServerReadcreate object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the key_server_readcreate. """

    connectivity = fields.Nested("netapp_ontap.models.key_server_state_array.KeyServerStateArraySchema", unknown=EXCLUDE, data_key="connectivity")
    r""" The connectivity field of the key_server_readcreate. """

    secondary_key_servers = fields.Str(data_key="secondary_key_servers")
    r""" A comma delimited string of the secondary key servers associated with the primary key server.

Example: secondary1.com, 10.2.3.4 """

    server = fields.Str(data_key="server")
    r""" External key server for key management. If no port is provided, a default port of 5696 is used.

Example: keyserver1.com:5698 """

    timeout = Size(data_key="timeout")
    r""" I/O timeout in seconds for communicating with the key server.

Example: 60 """

    username = fields.Str(data_key="username")
    r""" Username credentials for connecting with the key server.

Example: admin """

    @property
    def resource(self):
        return KeyServerReadcreate

    gettable_fields = [
        "links",
        "connectivity",
        "secondary_key_servers",
        "server",
        "timeout",
        "username",
    ]
    """links,connectivity,secondary_key_servers,server,timeout,username,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "server",
    ]
    """server,"""


class KeyServerReadcreate(Resource):

    _schema = KeyServerReadcreateSchema
