r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterPartner", "MetroclusterPartnerSchema"]
__pdoc__ = {
    "MetroclusterPartnerSchema.resource": False,
    "MetroclusterPartnerSchema.opts": False,
    "MetroclusterPartner": False,
}


class MetroclusterPartnerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterPartner object"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the metrocluster_partner. """

    type = fields.Str(data_key="type")
    r""" The type field of the metrocluster_partner.

Valid choices:

* ha
* dr
* aux """

    @property
    def resource(self):
        return MetroclusterPartner

    gettable_fields = [
        "node.links",
        "node.name",
        "node.uuid",
        "type",
    ]
    """node.links,node.name,node.uuid,type,"""

    patchable_fields = [
        "node.name",
        "node.uuid",
        "type",
    ]
    """node.name,node.uuid,type,"""

    postable_fields = [
        "node.name",
        "node.uuid",
        "type",
    ]
    """node.name,node.uuid,type,"""


class MetroclusterPartner(Resource):

    _schema = MetroclusterPartnerSchema
