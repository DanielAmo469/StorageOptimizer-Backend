r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesControllerFailedPowerSupplyMessage", "ClusterNodesControllerFailedPowerSupplyMessageSchema"]
__pdoc__ = {
    "ClusterNodesControllerFailedPowerSupplyMessageSchema.resource": False,
    "ClusterNodesControllerFailedPowerSupplyMessageSchema.opts": False,
    "ClusterNodesControllerFailedPowerSupplyMessage": False,
}


class ClusterNodesControllerFailedPowerSupplyMessageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesControllerFailedPowerSupplyMessage object"""

    code = fields.Str(data_key="code")
    r""" Error code describing the current condition of power supply.

Example: 111411208 """

    message = fields.Str(data_key="message")
    r""" Message describing the state of any power supplies that are currently degraded. It is only of use when `failed_power_supply.count` is not zero.

Example: There are no failed power supplies. """

    @property
    def resource(self):
        return ClusterNodesControllerFailedPowerSupplyMessage

    gettable_fields = [
        "code",
        "message",
    ]
    """code,message,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesControllerFailedPowerSupplyMessage(Resource):

    _schema = ClusterNodesControllerFailedPowerSupplyMessageSchema
