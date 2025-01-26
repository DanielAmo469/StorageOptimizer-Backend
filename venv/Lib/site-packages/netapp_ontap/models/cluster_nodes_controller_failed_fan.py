r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesControllerFailedFan", "ClusterNodesControllerFailedFanSchema"]
__pdoc__ = {
    "ClusterNodesControllerFailedFanSchema.resource": False,
    "ClusterNodesControllerFailedFanSchema.opts": False,
    "ClusterNodesControllerFailedFan": False,
}


class ClusterNodesControllerFailedFanSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesControllerFailedFan object"""

    count = Size(data_key="count")
    r""" Specifies a count of the number of chassis fans that are not operating within the recommended RPM range.

Example: 1 """

    message = fields.Nested("netapp_ontap.models.cluster_nodes_controller_failed_fan_message.ClusterNodesControllerFailedFanMessageSchema", unknown=EXCLUDE, data_key="message")
    r""" The message field of the cluster_nodes_controller_failed_fan. """

    @property
    def resource(self):
        return ClusterNodesControllerFailedFan

    gettable_fields = [
        "count",
        "message",
    ]
    """count,message,"""

    patchable_fields = [
        "message",
    ]
    """message,"""

    postable_fields = [
        "message",
    ]
    """message,"""


class ClusterNodesControllerFailedFan(Resource):

    _schema = ClusterNodesControllerFailedFanSchema
