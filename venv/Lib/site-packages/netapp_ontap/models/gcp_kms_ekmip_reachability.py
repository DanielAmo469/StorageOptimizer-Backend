r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["GcpKmsEkmipReachability", "GcpKmsEkmipReachabilitySchema"]
__pdoc__ = {
    "GcpKmsEkmipReachabilitySchema.resource": False,
    "GcpKmsEkmipReachabilitySchema.opts": False,
    "GcpKmsEkmipReachability": False,
}


class GcpKmsEkmipReachabilitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GcpKmsEkmipReachability object"""

    code = fields.Str(data_key="code")
    r""" Code corresponding to the error message. Returns a 0 if a given SVM is able to communicate to the EKMIP servers of all of the nodes in the cluster.

Example: 346758 """

    message = fields.Str(data_key="message")
    r""" Error message set when cluster-wide EKMIP server availability from the given SVM and node is false.

Example: embedded KMIP server status unavailable on node. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the gcp_kms_ekmip_reachability. """

    reachable = fields.Boolean(data_key="reachable")
    r""" Set to true if the given SVM on the given node is able to communicate to all EKMIP servers configured on all nodes in the cluster. """

    @property
    def resource(self):
        return GcpKmsEkmipReachability

    gettable_fields = [
        "code",
        "message",
        "node.links",
        "node.name",
        "node.uuid",
        "reachable",
    ]
    """code,message,node.links,node.name,node.uuid,reachable,"""

    patchable_fields = [
        "code",
        "message",
        "node.name",
        "node.uuid",
        "reachable",
    ]
    """code,message,node.name,node.uuid,reachable,"""

    postable_fields = [
        "code",
        "message",
        "node.name",
        "node.uuid",
        "reachable",
    ]
    """code,message,node.name,node.uuid,reachable,"""


class GcpKmsEkmipReachability(Resource):

    _schema = GcpKmsEkmipReachabilitySchema
