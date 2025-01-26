r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IgroupInitiatorConnectivityTrackingConnectionsLoginsInterface", "IgroupInitiatorConnectivityTrackingConnectionsLoginsInterfaceSchema"]
__pdoc__ = {
    "IgroupInitiatorConnectivityTrackingConnectionsLoginsInterfaceSchema.resource": False,
    "IgroupInitiatorConnectivityTrackingConnectionsLoginsInterfaceSchema.opts": False,
    "IgroupInitiatorConnectivityTrackingConnectionsLoginsInterface": False,
}


class IgroupInitiatorConnectivityTrackingConnectionsLoginsInterfaceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IgroupInitiatorConnectivityTrackingConnectionsLoginsInterface object"""

    fc = fields.Nested("netapp_ontap.resources.fc_interface.FcInterfaceSchema", unknown=EXCLUDE, data_key="fc")
    r""" The fc field of the igroup_initiator_connectivity_tracking_connections_logins_interface. """

    ip = fields.Nested("netapp_ontap.resources.ip_interface.IpInterfaceSchema", unknown=EXCLUDE, data_key="ip")
    r""" The ip field of the igroup_initiator_connectivity_tracking_connections_logins_interface. """

    @property
    def resource(self):
        return IgroupInitiatorConnectivityTrackingConnectionsLoginsInterface

    gettable_fields = [
        "fc.links",
        "fc.name",
        "fc.uuid",
        "fc.wwpn",
        "ip.links",
        "ip.ip",
        "ip.name",
        "ip.uuid",
    ]
    """fc.links,fc.name,fc.uuid,fc.wwpn,ip.links,ip.ip,ip.name,ip.uuid,"""

    patchable_fields = [
        "fc.name",
        "fc.uuid",
        "ip.ip",
        "ip.name",
        "ip.uuid",
    ]
    """fc.name,fc.uuid,ip.ip,ip.name,ip.uuid,"""

    postable_fields = [
        "fc.name",
        "fc.uuid",
        "ip.ip",
        "ip.name",
        "ip.uuid",
    ]
    """fc.name,fc.uuid,ip.ip,ip.name,ip.uuid,"""


class IgroupInitiatorConnectivityTrackingConnectionsLoginsInterface(Resource):

    _schema = IgroupInitiatorConnectivityTrackingConnectionsLoginsInterfaceSchema
