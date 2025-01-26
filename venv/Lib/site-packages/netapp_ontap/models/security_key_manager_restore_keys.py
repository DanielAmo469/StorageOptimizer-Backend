r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SecurityKeyManagerRestoreKeys", "SecurityKeyManagerRestoreKeysSchema"]
__pdoc__ = {
    "SecurityKeyManagerRestoreKeysSchema.resource": False,
    "SecurityKeyManagerRestoreKeysSchema.opts": False,
    "SecurityKeyManagerRestoreKeys": False,
}


class SecurityKeyManagerRestoreKeysSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SecurityKeyManagerRestoreKeys object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the security_key_manager_restore_keys. """

    key_id = fields.Str(data_key="key_id")
    r""" Key identifier.

Example: 000000000000000002000000000001003aa8ce6a4fea3e466620134bea9510a10000000000000000 """

    key_server = fields.Str(data_key="key_server")
    r""" External key server for key management.

Example: keyserver1.com:5698 """

    key_tag = fields.Str(data_key="key_tag")
    r""" Additional information associated with the key.

Example: Authentication-Key-01 """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the security_key_manager_restore_keys. """

    @property
    def resource(self):
        return SecurityKeyManagerRestoreKeys

    gettable_fields = [
        "links",
        "key_id",
        "key_server",
        "key_tag",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """links,key_id,key_server,key_tag,node.links,node.name,node.uuid,"""

    patchable_fields = [
        "key_id",
        "key_server",
        "key_tag",
        "node.name",
        "node.uuid",
    ]
    """key_id,key_server,key_tag,node.name,node.uuid,"""

    postable_fields = [
        "key_id",
        "key_server",
        "key_tag",
        "node.name",
        "node.uuid",
    ]
    """key_id,key_server,key_tag,node.name,node.uuid,"""


class SecurityKeyManagerRestoreKeys(Resource):

    _schema = SecurityKeyManagerRestoreKeysSchema
