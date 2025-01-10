r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["MetroclusterDiagConnectionDetails", "MetroclusterDiagConnectionDetailsSchema"]
__pdoc__ = {
    "MetroclusterDiagConnectionDetailsSchema.resource": False,
    "MetroclusterDiagConnectionDetailsSchema.opts": False,
    "MetroclusterDiagConnectionDetails": False,
}


class MetroclusterDiagConnectionDetailsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MetroclusterDiagConnectionDetails object"""

    cluster = fields.Nested("netapp_ontap.resources.cluster.ClusterSchema", unknown=EXCLUDE, data_key="cluster")
    r""" The cluster field of the metrocluster_diag_connection_details. """

    connections = fields.List(fields.Nested("netapp_ontap.models.metrocluster_diag_connection.MetroclusterDiagConnectionSchema", unknown=EXCLUDE), data_key="connections")
    r""" The connections field of the metrocluster_diag_connection_details. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the metrocluster_diag_connection_details. """

    @property
    def resource(self):
        return MetroclusterDiagConnectionDetails

    gettable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "connections",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,connections,node.links,node.name,node.uuid,"""

    patchable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "node.name",
        "node.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,node.name,node.uuid,"""

    postable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "node.name",
        "node.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,node.name,node.uuid,"""


class MetroclusterDiagConnectionDetails(Resource):

    _schema = MetroclusterDiagConnectionDetailsSchema
