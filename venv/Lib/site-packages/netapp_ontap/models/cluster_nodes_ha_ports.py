r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesHaPorts", "ClusterNodesHaPortsSchema"]
__pdoc__ = {
    "ClusterNodesHaPortsSchema.resource": False,
    "ClusterNodesHaPortsSchema.opts": False,
    "ClusterNodesHaPorts": False,
}


class ClusterNodesHaPortsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesHaPorts object"""

    number = Size(data_key="number")
    r""" HA port number

Example: 0 """

    state = fields.Str(data_key="state")
    r""" HA port state:

* <i>down</i> - Logical HA link is down.
* <i>initialized</i> - Logical HA link is initialized. The physical link is up, but the subnet manager hasn’t started to configure the port.
* <i>armed</i> - Logical HA link is armed. The physical link is up and the subnet manager started but did not yet complete configuring the port.
* <i>active</i> - Logical HA link is active.
* <i>reserved</i> - Logical HA link is active, but the physical link is down.


Valid choices:

* down
* initialized
* armed
* active
* reserved """

    @property
    def resource(self):
        return ClusterNodesHaPorts

    gettable_fields = [
        "number",
        "state",
    ]
    """number,state,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesHaPorts(Resource):

    _schema = ClusterNodesHaPortsSchema
