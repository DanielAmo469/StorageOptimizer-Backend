r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["Peer", "PeerSchema"]
__pdoc__ = {
    "PeerSchema.resource": False,
    "PeerSchema.opts": False,
    "Peer": False,
}


class PeerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Peer object"""

    cluster = fields.Nested("netapp_ontap.resources.cluster_peer.ClusterPeerSchema", unknown=EXCLUDE, data_key="cluster")
    r""" The cluster field of the peer. """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the peer. """

    @property
    def resource(self):
        return Peer

    gettable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,svm.name,svm.uuid,"""

    postable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,svm.name,svm.uuid,"""


class Peer(Resource):

    _schema = PeerSchema
