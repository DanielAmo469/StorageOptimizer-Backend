r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VolumeConstituentsAggregates", "VolumeConstituentsAggregatesSchema"]
__pdoc__ = {
    "VolumeConstituentsAggregatesSchema.resource": False,
    "VolumeConstituentsAggregatesSchema.opts": False,
    "VolumeConstituentsAggregates": False,
}


class VolumeConstituentsAggregatesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VolumeConstituentsAggregates object"""

    name = fields.Str(data_key="name")
    r""" Name of the aggregate hosting the FlexGroup Constituent. """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier for the aggregate.

Example: 028baa66-41bd-11e9-81d5-00a0986138f7 """

    @property
    def resource(self):
        return VolumeConstituentsAggregates

    gettable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class VolumeConstituentsAggregates(Resource):

    _schema = VolumeConstituentsAggregatesSchema
