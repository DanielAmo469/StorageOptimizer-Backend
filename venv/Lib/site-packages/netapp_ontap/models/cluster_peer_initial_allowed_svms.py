r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterPeerInitialAllowedSvms", "ClusterPeerInitialAllowedSvmsSchema"]
__pdoc__ = {
    "ClusterPeerInitialAllowedSvmsSchema.resource": False,
    "ClusterPeerInitialAllowedSvmsSchema.opts": False,
    "ClusterPeerInitialAllowedSvms": False,
}


class ClusterPeerInitialAllowedSvmsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterPeerInitialAllowedSvms object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the cluster_peer_initial_allowed_svms. """

    name = fields.Str(data_key="name")
    r""" The name of the SVM.


Example: svm1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the SVM.


Example: 02c9e252-41be-11e9-81d5-00a0986138f7 """

    @property
    def resource(self):
        return ClusterPeerInitialAllowedSvms

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


class ClusterPeerInitialAllowedSvms(Resource):

    _schema = ClusterPeerInitialAllowedSvmsSchema
