r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupVolumeActivityTracking", "ConsistencyGroupVolumeActivityTrackingSchema"]
__pdoc__ = {
    "ConsistencyGroupVolumeActivityTrackingSchema.resource": False,
    "ConsistencyGroupVolumeActivityTrackingSchema.opts": False,
    "ConsistencyGroupVolumeActivityTracking": False,
}


class ConsistencyGroupVolumeActivityTrackingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupVolumeActivityTracking object"""

    state = fields.Str(data_key="state")
    r""" Activity tracking state of the volume. If this value is _on_, ONTAP collects top metrics information for the volume in real time. There is a slight impact to I/O performance in order to collect this information. If this value is _off_, no activity tracking information is collected or available to view. The default value is _on_ for all volumes that support file system analytics. If the volume will contain LUNs or NVMe namespaces, the default value is _off_.

Valid choices:

* off
* on """

    supported = fields.Boolean(data_key="supported")
    r""" This field indicates whether or not volume activity tracking is supported on the volume. If volume activity tracking is not supported, the reason why is provided in the `activity_tracking.unsupported_reason` field. """

    unsupported_reason = fields.Nested("netapp_ontap.models.consistency_group_consistency_groups_volumes_activity_tracking_unsupported_reason.ConsistencyGroupConsistencyGroupsVolumesActivityTrackingUnsupportedReasonSchema", unknown=EXCLUDE, data_key="unsupported_reason")
    r""" The unsupported_reason field of the consistency_group_volume_activity_tracking. """

    @property
    def resource(self):
        return ConsistencyGroupVolumeActivityTracking

    gettable_fields = [
        "state",
        "supported",
        "unsupported_reason",
    ]
    """state,supported,unsupported_reason,"""

    patchable_fields = [
        "state",
        "unsupported_reason",
    ]
    """state,unsupported_reason,"""

    postable_fields = [
        "state",
        "unsupported_reason",
    ]
    """state,unsupported_reason,"""


class ConsistencyGroupVolumeActivityTracking(Resource):

    _schema = ConsistencyGroupVolumeActivityTrackingSchema
