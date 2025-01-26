r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupVolumeAnalytics", "ConsistencyGroupVolumeAnalyticsSchema"]
__pdoc__ = {
    "ConsistencyGroupVolumeAnalyticsSchema.resource": False,
    "ConsistencyGroupVolumeAnalyticsSchema.opts": False,
    "ConsistencyGroupVolumeAnalytics": False,
}


class ConsistencyGroupVolumeAnalyticsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupVolumeAnalytics object"""

    scan_progress = Size(data_key="scan_progress")
    r""" Percentage of files in the volume that the file system analytics initialization scan has processed. Only returned when the `state` is _initializing_.

Example: 17 """

    state = fields.Str(data_key="state")
    r""" File system analytics state of the volume. If this value is _on_, ONTAP collects extra file system analytics information for all directories on the volume. There will be a slight impact to I/O performance to collect this information. If this value is _off_, file system analytics information is not collected and not available to be viewed. If this value is _initializing_, that means file system analytics was recently turned on, and the initialization scan to gather information for all existing files and directories is currently running. If this value is _initialization_paused_, this means that the initialization scan is currently paused. If this value is 'unknown', this means that there was an internal error when determining the file system analytics state for the volume. The default value is _on_ for all volumes that support file system analytics. If the volume will contain LUNs or NVMe namespaces, the default value is _off_.

Valid choices:

* unknown
* initializing
* initialization_paused
* off
* on """

    supported = fields.Boolean(data_key="supported")
    r""" This field indicates whether or not file system analytics is supported on the volume. If file system analytics is not supported, the reason will be specified in the `analytics.unsupported_reason` field. """

    unsupported_reason = fields.Nested("netapp_ontap.models.consistency_group_consistency_groups_volumes_analytics_unsupported_reason.ConsistencyGroupConsistencyGroupsVolumesAnalyticsUnsupportedReasonSchema", unknown=EXCLUDE, data_key="unsupported_reason")
    r""" The unsupported_reason field of the consistency_group_volume_analytics. """

    @property
    def resource(self):
        return ConsistencyGroupVolumeAnalytics

    gettable_fields = [
        "scan_progress",
        "state",
        "supported",
        "unsupported_reason",
    ]
    """scan_progress,state,supported,unsupported_reason,"""

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


class ConsistencyGroupVolumeAnalytics(Resource):

    _schema = ConsistencyGroupVolumeAnalyticsSchema
