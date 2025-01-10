r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CapacityPoolNodes", "CapacityPoolNodesSchema"]
__pdoc__ = {
    "CapacityPoolNodesSchema.resource": False,
    "CapacityPoolNodesSchema.opts": False,
    "CapacityPoolNodes": False,
}


class CapacityPoolNodesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CapacityPoolNodes object"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the capacity_pool_nodes. """

    used_size = Size(data_key="used_size")
    r""" Capacity, in bytes, that is currently used by the node. """

    @property
    def resource(self):
        return CapacityPoolNodes

    gettable_fields = [
        "node.links",
        "node.name",
        "node.uuid",
        "used_size",
    ]
    """node.links,node.name,node.uuid,used_size,"""

    patchable_fields = [
        "node.name",
        "node.uuid",
    ]
    """node.name,node.uuid,"""

    postable_fields = [
        "node.name",
        "node.uuid",
    ]
    """node.name,node.uuid,"""


class CapacityPoolNodes(Resource):

    _schema = CapacityPoolNodesSchema
