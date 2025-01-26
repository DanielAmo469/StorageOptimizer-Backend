r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IgroupInitiatorConnectivityTrackingConnectionsLogins", "IgroupInitiatorConnectivityTrackingConnectionsLoginsSchema"]
__pdoc__ = {
    "IgroupInitiatorConnectivityTrackingConnectionsLoginsSchema.resource": False,
    "IgroupInitiatorConnectivityTrackingConnectionsLoginsSchema.opts": False,
    "IgroupInitiatorConnectivityTrackingConnectionsLogins": False,
}


class IgroupInitiatorConnectivityTrackingConnectionsLoginsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IgroupInitiatorConnectivityTrackingConnectionsLogins object"""

    connected = fields.Boolean(data_key="connected")
    r""" True if the initiator is currently logged in to this connection's interface. """

    interface = fields.Nested("netapp_ontap.models.igroup_initiator_connectivity_tracking_connections_logins_interface.IgroupInitiatorConnectivityTrackingConnectionsLoginsInterfaceSchema", unknown=EXCLUDE, data_key="interface")
    r""" The interface field of the igroup_initiator_connectivity_tracking_connections_logins. """

    last_seen_time = ImpreciseDateTime(data_key="last_seen_time")
    r""" The last time this initiator logged in. Logins not seen for 48 hours are cleared and not reported.

Example: 2021-03-14T05:19:00.000+0000 """

    @property
    def resource(self):
        return IgroupInitiatorConnectivityTrackingConnectionsLogins

    gettable_fields = [
        "connected",
        "interface",
        "last_seen_time",
    ]
    """connected,interface,last_seen_time,"""

    patchable_fields = [
        "interface",
    ]
    """interface,"""

    postable_fields = [
        "interface",
    ]
    """interface,"""


class IgroupInitiatorConnectivityTrackingConnectionsLogins(Resource):

    _schema = IgroupInitiatorConnectivityTrackingConnectionsLoginsSchema
