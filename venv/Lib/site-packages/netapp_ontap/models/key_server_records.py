r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["KeyServerRecords", "KeyServerRecordsSchema"]
__pdoc__ = {
    "KeyServerRecordsSchema.resource": False,
    "KeyServerRecordsSchema.opts": False,
    "KeyServerRecords": False,
}


class KeyServerRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the KeyServerRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the key_server_records. """

    connectivity = fields.Nested("netapp_ontap.models.key_server_state_array.KeyServerStateArraySchema", unknown=EXCLUDE, data_key="connectivity")
    r""" The connectivity field of the key_server_records. """

    password = fields.Str(data_key="password")
    r""" Password credentials for connecting with the key server. This is not audited.

Example: password """

    server = fields.Str(data_key="server")
    r""" External key server for key management. If no port is provided, a default port of 5696 is used. Not valid in POST if `records` is provided.

Example: bulkkeyserver.com:5698 """

    timeout = Size(data_key="timeout")
    r""" I/O timeout in seconds for communicating with the key server.

Example: 60 """

    username = fields.Str(data_key="username")
    r""" KMIP username credentials for connecting with the key server.

Example: username """

    @property
    def resource(self):
        return KeyServerRecords

    gettable_fields = [
        "links",
        "connectivity",
        "server",
        "timeout",
        "username",
    ]
    """links,connectivity,server,timeout,username,"""

    patchable_fields = [
        "password",
        "timeout",
        "username",
    ]
    """password,timeout,username,"""

    postable_fields = [
        "server",
    ]
    """server,"""


class KeyServerRecords(Resource):

    _schema = KeyServerRecordsSchema
