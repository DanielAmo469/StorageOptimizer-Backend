r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IgroupInitiatorConnectivityTrackingConnections", "IgroupInitiatorConnectivityTrackingConnectionsSchema"]
__pdoc__ = {
    "IgroupInitiatorConnectivityTrackingConnectionsSchema.resource": False,
    "IgroupInitiatorConnectivityTrackingConnectionsSchema.opts": False,
    "IgroupInitiatorConnectivityTrackingConnections": False,
}


class IgroupInitiatorConnectivityTrackingConnectionsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IgroupInitiatorConnectivityTrackingConnections object"""

    logins = fields.List(fields.Nested("netapp_ontap.models.igroup_initiator_connectivity_tracking_connections_logins.IgroupInitiatorConnectivityTrackingConnectionsLoginsSchema", unknown=EXCLUDE), data_key="logins")
    r""" The logins field of the igroup_initiator_connectivity_tracking_connections. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the igroup_initiator_connectivity_tracking_connections. """

    @property
    def resource(self):
        return IgroupInitiatorConnectivityTrackingConnections

    gettable_fields = [
        "logins",
        "node.links",
        "node.name",
        "node.uuid",
    ]
    """logins,node.links,node.name,node.uuid,"""

    patchable_fields = [
        "logins",
        "node.name",
        "node.uuid",
    ]
    """logins,node.name,node.uuid,"""

    postable_fields = [
        "logins",
        "node.name",
        "node.uuid",
    ]
    """logins,node.name,node.uuid,"""


class IgroupInitiatorConnectivityTrackingConnections(Resource):

    _schema = IgroupInitiatorConnectivityTrackingConnectionsSchema
