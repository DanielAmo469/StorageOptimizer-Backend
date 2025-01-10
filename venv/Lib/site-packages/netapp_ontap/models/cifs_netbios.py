r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsNetbios", "CifsNetbiosSchema"]
__pdoc__ = {
    "CifsNetbiosSchema.resource": False,
    "CifsNetbiosSchema.opts": False,
    "CifsNetbios": False,
}


class CifsNetbiosSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsNetbios object"""

    aliases = fields.List(fields.Str, data_key="aliases")
    r""" The aliases field of the cifs_netbios.

Example: ["ALIAS_1","ALIAS_2","ALIAS_3"] """

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether NetBios name service (NBNS) is enabled for the CIFS. If this service is enabled, the CIFS server will start sending the broadcast for name registration. """

    wins_servers = fields.List(fields.Str, data_key="wins_servers")
    r""" The wins_servers field of the cifs_netbios.

Example: ["10.224.65.20","10.224.65.21"] """

    @property
    def resource(self):
        return CifsNetbios

    gettable_fields = [
        "aliases",
        "enabled",
        "wins_servers",
    ]
    """aliases,enabled,wins_servers,"""

    patchable_fields = [
        "aliases",
        "enabled",
        "wins_servers",
    ]
    """aliases,enabled,wins_servers,"""

    postable_fields = [
        "aliases",
        "enabled",
        "wins_servers",
    ]
    """aliases,enabled,wins_servers,"""


class CifsNetbios(Resource):

    _schema = CifsNetbiosSchema
