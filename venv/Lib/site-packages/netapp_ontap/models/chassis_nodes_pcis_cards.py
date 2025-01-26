r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ChassisNodesPcisCards", "ChassisNodesPcisCardsSchema"]
__pdoc__ = {
    "ChassisNodesPcisCardsSchema.resource": False,
    "ChassisNodesPcisCardsSchema.opts": False,
    "ChassisNodesPcisCards": False,
}


class ChassisNodesPcisCardsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ChassisNodesPcisCards object"""

    device = fields.Str(data_key="device")
    r""" The description of the PCI card.

Example: Intel Lewisburg series chipset SATA Controller """

    info = fields.Str(data_key="info")
    r""" The info string from the device driver of the PCI card.

Example: Additional Info: 0 (0xaaf00000)   SHM2S86Q120GLM22NP FW1146 114473MB 512B/sect (SPG190108GW) """

    slot = fields.Str(data_key="slot")
    r""" The slot where the PCI card is placed. This can sometimes take the form of "6-1" to indicate slot and subslot.

Example: 0 """

    @property
    def resource(self):
        return ChassisNodesPcisCards

    gettable_fields = [
        "device",
        "info",
        "slot",
    ]
    """device,info,slot,"""

    patchable_fields = [
        "device",
        "info",
        "slot",
    ]
    """device,info,slot,"""

    postable_fields = [
        "device",
        "info",
        "slot",
    ]
    """device,info,slot,"""


class ChassisNodesPcisCards(Resource):

    _schema = ChassisNodesPcisCardsSchema
