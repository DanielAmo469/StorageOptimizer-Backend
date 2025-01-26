r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterPeerLinks", "ClusterPeerLinksSchema"]
__pdoc__ = {
    "ClusterPeerLinksSchema.resource": False,
    "ClusterPeerLinksSchema.opts": False,
    "ClusterPeerLinks": False,
}


class ClusterPeerLinksSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterPeerLinks object"""

    interfaces = fields.Nested("netapp_ontap.models.href.HrefSchema", unknown=EXCLUDE, data_key="interfaces")
    r""" The interfaces field of the cluster_peer_links. """

    self_ = fields.Nested("netapp_ontap.models.href.HrefSchema", unknown=EXCLUDE, data_key="self")
    r""" The self_ field of the cluster_peer_links. """

    @property
    def resource(self):
        return ClusterPeerLinks

    gettable_fields = [
        "interfaces",
        "self_",
    ]
    """interfaces,self_,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterPeerLinks(Resource):

    _schema = ClusterPeerLinksSchema
