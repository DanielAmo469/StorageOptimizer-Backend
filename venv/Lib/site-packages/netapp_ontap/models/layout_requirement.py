r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LayoutRequirement", "LayoutRequirementSchema"]
__pdoc__ = {
    "LayoutRequirementSchema.resource": False,
    "LayoutRequirementSchema.opts": False,
    "LayoutRequirement": False,
}


class LayoutRequirementSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LayoutRequirement object"""

    aggregate_min_disks = Size(data_key="aggregate_min_disks")
    r""" Minimum number of disks to create an aggregate.

Example: 6 """

    default = fields.Boolean(data_key="default")
    r""" Indicates if this RAID type is the default. """

    raid_group = fields.Nested("netapp_ontap.models.layout_requirement_raid_group.LayoutRequirementRaidGroupSchema", unknown=EXCLUDE, data_key="raid_group")
    r""" The raid_group field of the layout_requirement. """

    raid_type = fields.Str(data_key="raid_type")
    r""" RAID type.

Valid choices:

* raid_dp
* raid_tec
* raid4
* raid0 """

    @property
    def resource(self):
        return LayoutRequirement

    gettable_fields = [
        "aggregate_min_disks",
        "default",
        "raid_group",
        "raid_type",
    ]
    """aggregate_min_disks,default,raid_group,raid_type,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class LayoutRequirement(Resource):

    _schema = LayoutRequirementSchema
