r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfPaths", "ShelfPathsSchema"]
__pdoc__ = {
    "ShelfPathsSchema.resource": False,
    "ShelfPathsSchema.opts": False,
    "ShelfPaths": False,
}


class ShelfPathsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfPaths object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the shelf_paths. """

    name = fields.Str(data_key="name")
    r""" The name field of the shelf_paths.

Example: 2a """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the shelf_paths. """

    @property
    def resource(self):
        return ShelfPaths

    gettable_fields = [
        "links",
        "name",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """links,name,node.links,node.name,node.uuid,"""

    patchable_fields = [
        "name",
        "node.name",
        "node.uuid",
    ]
    """name,node.name,node.uuid,"""

    postable_fields = [
        "name",
        "node.name",
        "node.uuid",
    ]
    """name,node.name,node.uuid,"""


class ShelfPaths(Resource):

    _schema = ShelfPathsSchema
