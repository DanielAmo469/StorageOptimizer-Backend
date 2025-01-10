r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsVolumesNasJunctionParent", "ConsistencyGroupConsistencyGroupsVolumesNasJunctionParentSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsVolumesNasJunctionParentSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsVolumesNasJunctionParentSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsVolumesNasJunctionParent": False,
}


class ConsistencyGroupConsistencyGroupsVolumesNasJunctionParentSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsVolumesNasJunctionParent object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the consistency_group_consistency_groups_volumes_nas_junction_parent. """

    name = fields.Str(data_key="name")
    r""" The name of the parent volume that contains the junction inode of this volume. The junction parent volume must belong to the same SVM that owns this volume.

Example: vs1_root """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier for the parent volume.

Example: 75c9cfb0-3eb4-11eb-9fb4-005056bb088a """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsVolumesNasJunctionParent

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class ConsistencyGroupConsistencyGroupsVolumesNasJunctionParent(Resource):

    _schema = ConsistencyGroupConsistencyGroupsVolumesNasJunctionParentSchema
