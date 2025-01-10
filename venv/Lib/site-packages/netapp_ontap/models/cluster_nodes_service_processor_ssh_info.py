r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesServiceProcessorSshInfo", "ClusterNodesServiceProcessorSshInfoSchema"]
__pdoc__ = {
    "ClusterNodesServiceProcessorSshInfoSchema.resource": False,
    "ClusterNodesServiceProcessorSshInfoSchema.opts": False,
    "ClusterNodesServiceProcessorSshInfo": False,
}


class ClusterNodesServiceProcessorSshInfoSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesServiceProcessorSshInfo object"""

    allowed_addresses = fields.List(fields.Str, data_key="allowed_addresses")
    r""" Allowed IP addresses """

    @property
    def resource(self):
        return ClusterNodesServiceProcessorSshInfo

    gettable_fields = [
        "allowed_addresses",
    ]
    """allowed_addresses,"""

    patchable_fields = [
        "allowed_addresses",
    ]
    """allowed_addresses,"""

    postable_fields = [
        "allowed_addresses",
    ]
    """allowed_addresses,"""


class ClusterNodesServiceProcessorSshInfo(Resource):

    _schema = ClusterNodesServiceProcessorSshInfoSchema
