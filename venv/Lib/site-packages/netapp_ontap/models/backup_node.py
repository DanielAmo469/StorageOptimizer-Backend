r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["BackupNode", "BackupNodeSchema"]
__pdoc__ = {
    "BackupNodeSchema.resource": False,
    "BackupNodeSchema.opts": False,
    "BackupNode": False,
}


class BackupNodeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the BackupNode object"""

    name = fields.Str(data_key="name")
    r""" The name field of the backup_node. """

    @property
    def resource(self):
        return BackupNode

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


class BackupNode(Resource):

    _schema = BackupNodeSchema
