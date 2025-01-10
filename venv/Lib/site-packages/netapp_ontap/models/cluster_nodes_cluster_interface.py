r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesClusterInterface", "ClusterNodesClusterInterfaceSchema"]
__pdoc__ = {
    "ClusterNodesClusterInterfaceSchema.resource": False,
    "ClusterNodesClusterInterfaceSchema.opts": False,
    "ClusterNodesClusterInterface": False,
}


class ClusterNodesClusterInterfaceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesClusterInterface object"""

    ip = fields.Nested("netapp_ontap.models.node_setup_ip.NodeSetupIpSchema", unknown=EXCLUDE, data_key="ip")
    r""" The ip field of the cluster_nodes_cluster_interface. """

    @property
    def resource(self):
        return ClusterNodesClusterInterface

    gettable_fields = [
    ]
    """"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "ip",
    ]
    """ip,"""


class ClusterNodesClusterInterface(Resource):

    _schema = ClusterNodesClusterInterfaceSchema
