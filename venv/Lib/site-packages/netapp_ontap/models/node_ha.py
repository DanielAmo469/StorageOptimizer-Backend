r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NodeHa", "NodeHaSchema"]
__pdoc__ = {
    "NodeHaSchema.resource": False,
    "NodeHaSchema.opts": False,
    "NodeHa": False,
}


class NodeHaSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NodeHa object"""

    auto_giveback = fields.Boolean(data_key="auto_giveback")
    r""" Specifies whether giveback is automatically initiated when the node that owns the storage is ready. """

    enabled = fields.Boolean(data_key="enabled")
    r""" Specifies whether or not storage failover is enabled. """

    giveback = fields.Nested("netapp_ontap.models.node_ha_giveback.NodeHaGivebackSchema", unknown=EXCLUDE, data_key="giveback")
    r""" The giveback field of the node_ha. """

    interconnect = fields.Nested("netapp_ontap.models.cluster_nodes_ha_interconnect.ClusterNodesHaInterconnectSchema", unknown=EXCLUDE, data_key="interconnect")
    r""" The interconnect field of the node_ha. """

    partners = fields.List(fields.Nested("netapp_ontap.models.igroup_connectivity_tracking_required_nodes.IgroupConnectivityTrackingRequiredNodesSchema", unknown=EXCLUDE), data_key="partners")
    r""" Nodes in this node's High Availability (HA) group. """

    ports = fields.List(fields.Nested("netapp_ontap.models.cluster_nodes_ha_ports.ClusterNodesHaPortsSchema", unknown=EXCLUDE), data_key="ports")
    r""" The ports field of the node_ha. """

    takeover = fields.Nested("netapp_ontap.models.cluster_nodes_ha_takeover.ClusterNodesHaTakeoverSchema", unknown=EXCLUDE, data_key="takeover")
    r""" The takeover field of the node_ha. """

    @property
    def resource(self):
        return NodeHa

    gettable_fields = [
        "auto_giveback",
        "enabled",
        "giveback",
        "interconnect",
        "partners.links",
        "partners.name",
        "partners.uuid",
        "ports",
        "takeover",
    ]
    """auto_giveback,enabled,giveback,interconnect,partners.links,partners.name,partners.uuid,ports,takeover,"""

    patchable_fields = [
        "auto_giveback",
        "enabled",
        "giveback",
        "interconnect",
        "takeover",
    ]
    """auto_giveback,enabled,giveback,interconnect,takeover,"""

    postable_fields = [
        "auto_giveback",
        "enabled",
        "giveback",
        "interconnect",
        "takeover",
    ]
    """auto_giveback,enabled,giveback,interconnect,takeover,"""


class NodeHa(Resource):

    _schema = NodeHaSchema
