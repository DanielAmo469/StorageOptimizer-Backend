r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CapacityPoolResponseRecordsNodes", "CapacityPoolResponseRecordsNodesSchema"]
__pdoc__ = {
    "CapacityPoolResponseRecordsNodesSchema.resource": False,
    "CapacityPoolResponseRecordsNodesSchema.opts": False,
    "CapacityPoolResponseRecordsNodes": False,
}


class CapacityPoolResponseRecordsNodesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CapacityPoolResponseRecordsNodes object"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the capacity_pool_response_records_nodes. """

    used_size = Size(data_key="used_size")
    r""" Capacity, in bytes, that is currently used by the node. """

    @property
    def resource(self):
        return CapacityPoolResponseRecordsNodes

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


class CapacityPoolResponseRecordsNodes(Resource):

    _schema = CapacityPoolResponseRecordsNodesSchema
