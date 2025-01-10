r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesControllerFailedPowerSupply", "ClusterNodesControllerFailedPowerSupplySchema"]
__pdoc__ = {
    "ClusterNodesControllerFailedPowerSupplySchema.resource": False,
    "ClusterNodesControllerFailedPowerSupplySchema.opts": False,
    "ClusterNodesControllerFailedPowerSupply": False,
}


class ClusterNodesControllerFailedPowerSupplySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesControllerFailedPowerSupply object"""

    count = Size(data_key="count")
    r""" Number of failed power supply units.

Example: 1 """

    message = fields.Nested("netapp_ontap.models.cluster_nodes_controller_failed_power_supply_message.ClusterNodesControllerFailedPowerSupplyMessageSchema", unknown=EXCLUDE, data_key="message")
    r""" The message field of the cluster_nodes_controller_failed_power_supply. """

    @property
    def resource(self):
        return ClusterNodesControllerFailedPowerSupply

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


class ClusterNodesControllerFailedPowerSupply(Resource):

    _schema = ClusterNodesControllerFailedPowerSupplySchema
