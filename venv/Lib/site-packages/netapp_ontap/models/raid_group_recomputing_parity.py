r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["RaidGroupRecomputingParity", "RaidGroupRecomputingParitySchema"]
__pdoc__ = {
    "RaidGroupRecomputingParitySchema.resource": False,
    "RaidGroupRecomputingParitySchema.opts": False,
    "RaidGroupRecomputingParity": False,
}


class RaidGroupRecomputingParitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the RaidGroupRecomputingParity object"""

    active = fields.Boolean(data_key="active")
    r""" RAID group is recomputing parity """

    percent = Size(data_key="percent")
    r""" Recomputing parity percentage

Example: 10 """

    @property
    def resource(self):
        return RaidGroupRecomputingParity

    gettable_fields = [
        "active",
        "percent",
    ]
    """active,percent,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class RaidGroupRecomputingParity(Resource):

    _schema = RaidGroupRecomputingParitySchema
