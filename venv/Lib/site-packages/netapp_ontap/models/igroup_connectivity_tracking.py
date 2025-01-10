r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IgroupConnectivityTracking", "IgroupConnectivityTrackingSchema"]
__pdoc__ = {
    "IgroupConnectivityTrackingSchema.resource": False,
    "IgroupConnectivityTrackingSchema.opts": False,
    "IgroupConnectivityTracking": False,
}


class IgroupConnectivityTrackingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IgroupConnectivityTracking object"""

    alerts = fields.List(fields.Nested("netapp_ontap.models.igroup_connectivity_tracking_alerts.IgroupConnectivityTrackingAlertsSchema", unknown=EXCLUDE), data_key="alerts")
    r""" The alerts field of the igroup_connectivity_tracking. """

    connection_state = fields.Str(data_key="connection_state")
    r""" Connection state.

Valid choices:

* full
* none
* partial
* no_initiators
* no_lun_maps """

    required_nodes = fields.List(fields.Nested("netapp_ontap.models.cluster_nodes_ha_partners.ClusterNodesHaPartnersSchema", unknown=EXCLUDE), data_key="required_nodes")
    r""" Nodes to which the initiators in this group should be connected to ensure reliable service. This is the collection of any node hosting a LUN mapped to this igroup as well as the HA partners of those nodes. """

    @property
    def resource(self):
        return IgroupConnectivityTracking

    gettable_fields = [
        "alerts",
        "connection_state",
        "required_nodes.links",
        "required_nodes.name",
        "required_nodes.uuid",
    ]
    """alerts,connection_state,required_nodes.links,required_nodes.name,required_nodes.uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class IgroupConnectivityTracking(Resource):

    _schema = IgroupConnectivityTrackingSchema
