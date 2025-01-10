r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHwAssistStatus", "ClusterNodesHwAssistStatusSchema"]
__pdoc__ = {
    "ClusterNodesHwAssistStatusSchema.resource": False,
    "ClusterNodesHwAssistStatusSchema.opts": False,
    "ClusterNodesHwAssistStatus": False,
}


class ClusterNodesHwAssistStatusSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHwAssistStatus object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Indicates whether hardware assist is enabled on the node. """

    local = fields.Nested("netapp_ontap.models.hw_assist_status.HwAssistStatusSchema", unknown=EXCLUDE, data_key="local")
    r""" The local field of the cluster_nodes_hw_assist_status. """

    partner = fields.Nested("netapp_ontap.models.hw_assist_status.HwAssistStatusSchema", unknown=EXCLUDE, data_key="partner")
    r""" The partner field of the cluster_nodes_hw_assist_status. """

    @property
    def resource(self):
        return ClusterNodesHwAssistStatus

    gettable_fields = [
        "enabled",
        "local",
        "partner",
    ]
    """enabled,local,partner,"""

    patchable_fields = [
        "enabled",
    ]
    """enabled,"""

    postable_fields = [
        "enabled",
    ]
    """enabled,"""


class ClusterNodesHwAssistStatus(Resource):

    _schema = ClusterNodesHwAssistStatusSchema
