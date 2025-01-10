r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LayoutRequirementRaidGroup", "LayoutRequirementRaidGroupSchema"]
__pdoc__ = {
    "LayoutRequirementRaidGroupSchema.resource": False,
    "LayoutRequirementRaidGroupSchema.opts": False,
    "LayoutRequirementRaidGroup": False,
}


class LayoutRequirementRaidGroupSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LayoutRequirementRaidGroup object"""

    default = Size(data_key="default")
    r""" Default number of disks in a RAID group.

Example: 16 """

    max = Size(data_key="max")
    r""" Maximum number of disks allowed in a RAID group.

Example: 28 """

    min = Size(data_key="min")
    r""" Minimum number of disks allowed in a RAID group.

Example: 5 """

    @property
    def resource(self):
        return LayoutRequirementRaidGroup

    gettable_fields = [
        "default",
        "max",
        "min",
    ]
    """default,max,min,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class LayoutRequirementRaidGroup(Resource):

    _schema = LayoutRequirementRaidGroupSchema
