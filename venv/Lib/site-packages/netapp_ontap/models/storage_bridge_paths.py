r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["StorageBridgePaths", "StorageBridgePathsSchema"]
__pdoc__ = {
    "StorageBridgePathsSchema.resource": False,
    "StorageBridgePathsSchema.opts": False,
    "StorageBridgePaths": False,
}


class StorageBridgePathsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the StorageBridgePaths object"""

    name = fields.Str(data_key="name")
    r""" The name field of the storage_bridge_paths.

Example: 2c """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the storage_bridge_paths. """

    source_port = fields.Nested("netapp_ontap.models.storage_bridge_paths_source_port.StorageBridgePathsSourcePortSchema", unknown=EXCLUDE, data_key="source_port")
    r""" The source_port field of the storage_bridge_paths. """

    target_port = fields.Nested("netapp_ontap.models.storage_bridge_paths_target_port.StorageBridgePathsTargetPortSchema", unknown=EXCLUDE, data_key="target_port")
    r""" The target_port field of the storage_bridge_paths. """

    @property
    def resource(self):
        return StorageBridgePaths

    gettable_fields = [
        "name",
        "node.links",
        "node.name",
        "node.uuid",
        "source_port",
        "target_port",
    ]
    """name,node.links,node.name,node.uuid,source_port,target_port,"""

    patchable_fields = [
        "name",
        "node.name",
        "node.uuid",
        "source_port",
        "target_port",
    ]
    """name,node.name,node.uuid,source_port,target_port,"""

    postable_fields = [
        "name",
        "node.name",
        "node.uuid",
        "source_port",
        "target_port",
    ]
    """name,node.name,node.uuid,source_port,target_port,"""


class StorageBridgePaths(Resource):

    _schema = StorageBridgePathsSchema
