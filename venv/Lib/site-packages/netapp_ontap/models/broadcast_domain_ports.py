r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["BroadcastDomainPorts", "BroadcastDomainPortsSchema"]
__pdoc__ = {
    "BroadcastDomainPortsSchema.resource": False,
    "BroadcastDomainPortsSchema.opts": False,
    "BroadcastDomainPorts": False,
}


class BroadcastDomainPortsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the BroadcastDomainPorts object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the broadcast_domain_ports. """

    name = fields.Str(data_key="name")
    r""" The name field of the broadcast_domain_ports.

Example: e1b """

    node = fields.Nested("netapp_ontap.models.bgp_peer_group_local_port_node.BgpPeerGroupLocalPortNodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the broadcast_domain_ports. """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the broadcast_domain_ports.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return BroadcastDomainPorts

    gettable_fields = [
        "links",
        "name",
        "node",
        "uuid",
    ]
    """links,name,node,uuid,"""

    patchable_fields = [
        "name",
        "node",
        "uuid",
    ]
    """name,node,uuid,"""

    postable_fields = [
        "name",
        "node",
        "uuid",
    ]
    """name,node,uuid,"""


class BroadcastDomainPorts(Resource):

    _schema = BroadcastDomainPortsSchema
