r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesMetroclusterPorts", "ClusterNodesMetroclusterPortsSchema"]
__pdoc__ = {
    "ClusterNodesMetroclusterPortsSchema.resource": False,
    "ClusterNodesMetroclusterPortsSchema.opts": False,
    "ClusterNodesMetroclusterPorts": False,
}


class ClusterNodesMetroclusterPortsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesMetroclusterPorts object"""

    name = fields.Str(data_key="name")
    r""" The name field of the cluster_nodes_metrocluster_ports.

Example: e1b """

    @property
    def resource(self):
        return ClusterNodesMetroclusterPorts

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class ClusterNodesMetroclusterPorts(Resource):

    _schema = ClusterNodesMetroclusterPortsSchema
