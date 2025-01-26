r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FabricConnectionsClusterPortNode", "FabricConnectionsClusterPortNodeSchema"]
__pdoc__ = {
    "FabricConnectionsClusterPortNodeSchema.resource": False,
    "FabricConnectionsClusterPortNodeSchema.opts": False,
    "FabricConnectionsClusterPortNode": False,
}


class FabricConnectionsClusterPortNodeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FabricConnectionsClusterPortNode object"""

    name = fields.Str(data_key="name")
    r""" The name of the node on which the cluster Fibre Channel port is located.


Example: node1 """

    @property
    def resource(self):
        return FabricConnectionsClusterPortNode

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class FabricConnectionsClusterPortNode(Resource):

    _schema = FabricConnectionsClusterPortNodeSchema
