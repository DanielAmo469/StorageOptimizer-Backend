r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHaPartners", "ClusterNodesHaPartnersSchema"]
__pdoc__ = {
    "ClusterNodesHaPartnersSchema.resource": False,
    "ClusterNodesHaPartnersSchema.opts": False,
    "ClusterNodesHaPartners": False,
}


class ClusterNodesHaPartnersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHaPartners object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the cluster_nodes_ha_partners. """

    name = fields.Str(data_key="name")
    r""" The name field of the cluster_nodes_ha_partners.

Example: node1 """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the cluster_nodes_ha_partners.

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return ClusterNodesHaPartners

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class ClusterNodesHaPartners(Resource):

    _schema = ClusterNodesHaPartnersSchema
