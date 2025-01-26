r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHaInterconnect", "ClusterNodesHaInterconnectSchema"]
__pdoc__ = {
    "ClusterNodesHaInterconnectSchema.resource": False,
    "ClusterNodesHaInterconnectSchema.opts": False,
    "ClusterNodesHaInterconnect": False,
}


class ClusterNodesHaInterconnectSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHaInterconnect object"""

    adapter = fields.Str(data_key="adapter")
    r""" HA interconnect device name.

Example: MVIA-RDMA """

    state = fields.Str(data_key="state")
    r""" Indicates the HA interconnect status.

Valid choices:

* down
* up """

    @property
    def resource(self):
        return ClusterNodesHaInterconnect

    gettable_fields = [
        "adapter",
        "state",
    ]
    """adapter,state,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesHaInterconnect(Resource):

    _schema = ClusterNodesHaInterconnectSchema
