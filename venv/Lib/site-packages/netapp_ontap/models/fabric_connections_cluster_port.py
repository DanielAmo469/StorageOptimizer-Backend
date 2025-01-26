r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FabricConnectionsClusterPort", "FabricConnectionsClusterPortSchema"]
__pdoc__ = {
    "FabricConnectionsClusterPortSchema.resource": False,
    "FabricConnectionsClusterPortSchema.opts": False,
    "FabricConnectionsClusterPort": False,
}


class FabricConnectionsClusterPortSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FabricConnectionsClusterPort object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the fabric_connections_cluster_port. """

    name = fields.Str(data_key="name")
    r""" The name of the cluster Fibre Channel port.


Example: 0a """

    node = fields.Nested("netapp_ontap.models.fabric_connections_cluster_port_node.FabricConnectionsClusterPortNodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the fabric_connections_cluster_port. """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the cluster Fibre Channel port.


Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    wwpn = fields.Str(data_key="wwpn")
    r""" The world wide port name (WWPN) of the cluster Fibre Channel port.


Example: 50:0a:11:22:33:44:55:66 """

    @property
    def resource(self):
        return FabricConnectionsClusterPort

    gettable_fields = [
        "links",
        "name",
        "node",
        "uuid",
        "wwpn",
    ]
    """links,name,node,uuid,wwpn,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class FabricConnectionsClusterPort(Resource):

    _schema = FabricConnectionsClusterPortSchema
