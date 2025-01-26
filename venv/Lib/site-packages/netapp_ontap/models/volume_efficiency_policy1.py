r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeEfficiencyPolicy1", "VolumeEfficiencyPolicy1Schema"]
__pdoc__ = {
    "VolumeEfficiencyPolicy1Schema.resource": False,
    "VolumeEfficiencyPolicy1Schema.opts": False,
    "VolumeEfficiencyPolicy1": False,
}


class VolumeEfficiencyPolicy1Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeEfficiencyPolicy1 object"""

    name = fields.Str(data_key="name")
    r""" Specifies the name of the efficiency policy. """

    @property
    def resource(self):
        return VolumeEfficiencyPolicy1

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


class VolumeEfficiencyPolicy1(Resource):

    _schema = VolumeEfficiencyPolicy1Schema
