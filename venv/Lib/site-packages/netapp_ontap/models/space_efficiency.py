r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SpaceEfficiency", "SpaceEfficiencySchema"]
__pdoc__ = {
    "SpaceEfficiencySchema.resource": False,
    "SpaceEfficiencySchema.opts": False,
    "SpaceEfficiency": False,
}


class SpaceEfficiencySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SpaceEfficiency object"""

    logical_used = Size(data_key="logical_used")
    r""" Logical used """

    ratio = fields.Number(data_key="ratio")
    r""" Data reduction ratio (logical_used / used) """

    savings = Size(data_key="savings")
    r""" Space saved by storage efficiencies (logical_used - used) """

    @property
    def resource(self):
        return SpaceEfficiency

    gettable_fields = [
        "logical_used",
        "ratio",
        "savings",
    ]
    """logical_used,ratio,savings,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class SpaceEfficiency(Resource):

    _schema = SpaceEfficiencySchema
