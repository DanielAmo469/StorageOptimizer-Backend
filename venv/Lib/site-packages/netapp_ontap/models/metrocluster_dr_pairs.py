r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterDrPairs", "MetroclusterDrPairsSchema"]
__pdoc__ = {
    "MetroclusterDrPairsSchema.resource": False,
    "MetroclusterDrPairsSchema.opts": False,
    "MetroclusterDrPairs": False,
}


class MetroclusterDrPairsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterDrPairs object"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the metrocluster_dr_pairs. """

    partner = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="partner")
    r""" The partner field of the metrocluster_dr_pairs. """

    @property
    def resource(self):
        return MetroclusterDrPairs

    gettable_fields = [
        "node.links",
        "node.name",
        "node.uuid",
        "partner.links",
        "partner.name",
        "partner.uuid",
    ]
    """node.links,node.name,node.uuid,partner.links,partner.name,partner.uuid,"""

    patchable_fields = [
        "node.name",
        "node.uuid",
        "partner.name",
        "partner.uuid",
    ]
    """node.name,node.uuid,partner.name,partner.uuid,"""

    postable_fields = [
        "node.name",
        "node.uuid",
        "partner.name",
        "partner.uuid",
    ]
    """node.name,node.uuid,partner.name,partner.uuid,"""


class MetroclusterDrPairs(Resource):

    _schema = MetroclusterDrPairsSchema
