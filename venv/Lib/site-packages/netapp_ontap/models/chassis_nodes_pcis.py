r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ChassisNodesPcis", "ChassisNodesPcisSchema"]
__pdoc__ = {
    "ChassisNodesPcisSchema.resource": False,
    "ChassisNodesPcisSchema.opts": False,
    "ChassisNodesPcis": False,
}


class ChassisNodesPcisSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ChassisNodesPcis object"""

    cards = fields.List(fields.Nested("netapp_ontap.models.chassis_nodes_pcis_cards.ChassisNodesPcisCardsSchema", unknown=EXCLUDE), data_key="cards")
    r""" The cards field of the chassis_nodes_pcis. """

    @property
    def resource(self):
        return ChassisNodesPcis

    gettable_fields = [
        "cards",
    ]
    """cards,"""

    patchable_fields = [
        "cards",
    ]
    """cards,"""

    postable_fields = [
        "cards",
    ]
    """cards,"""


class ChassisNodesPcis(Resource):

    _schema = ChassisNodesPcisSchema
