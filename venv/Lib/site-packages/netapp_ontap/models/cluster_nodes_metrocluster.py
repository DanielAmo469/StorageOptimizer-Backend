r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesMetrocluster", "ClusterNodesMetroclusterSchema"]
__pdoc__ = {
    "ClusterNodesMetroclusterSchema.resource": False,
    "ClusterNodesMetroclusterSchema.opts": False,
    "ClusterNodesMetrocluster": False,
}


class ClusterNodesMetroclusterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesMetrocluster object"""

    custom_vlan_capable = fields.Boolean(data_key="custom_vlan_capable")
    r""" Indicates whether the MetroCluster over IP platform supports custom VLAN IDs. """

    ports = fields.List(fields.Nested("netapp_ontap.models.cluster_nodes_metrocluster_ports.ClusterNodesMetroclusterPortsSchema", unknown=EXCLUDE), data_key="ports")
    r""" MetroCluster over IP ports. """

    type = fields.Str(data_key="type")
    r""" The Metrocluster configuration type

Valid choices:

* fc
* fc_2_node
* ip """

    @property
    def resource(self):
        return ClusterNodesMetrocluster

    gettable_fields = [
        "custom_vlan_capable",
        "ports",
        "type",
    ]
    """custom_vlan_capable,ports,type,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesMetrocluster(Resource):

    _schema = ClusterNodesMetroclusterSchema
