r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesControllerFrus", "ClusterNodesControllerFrusSchema"]
__pdoc__ = {
    "ClusterNodesControllerFrusSchema.resource": False,
    "ClusterNodesControllerFrusSchema.opts": False,
    "ClusterNodesControllerFrus": False,
}


class ClusterNodesControllerFrusSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesControllerFrus object"""

    id = fields.Str(data_key="id")
    r""" The id field of the cluster_nodes_controller_frus. """

    state = fields.Str(data_key="state")
    r""" The state field of the cluster_nodes_controller_frus.

Valid choices:

* ok
* error """

    type = fields.Str(data_key="type")
    r""" The type field of the cluster_nodes_controller_frus.

Valid choices:

* fan
* psu
* pcie
* disk
* nvs
* dimm
* controller """

    @property
    def resource(self):
        return ClusterNodesControllerFrus

    gettable_fields = [
        "id",
        "state",
        "type",
    ]
    """id,state,type,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesControllerFrus(Resource):

    _schema = ClusterNodesControllerFrusSchema
