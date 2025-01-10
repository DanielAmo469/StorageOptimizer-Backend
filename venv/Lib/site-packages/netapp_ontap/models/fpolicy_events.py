r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FpolicyEvents", "FpolicyEventsSchema"]
__pdoc__ = {
    "FpolicyEventsSchema.resource": False,
    "FpolicyEventsSchema.opts": False,
    "FpolicyEvents": False,
}


class FpolicyEventsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FpolicyEvents object"""

    file_operations = fields.Nested("netapp_ontap.models.fpolicy_event_file_operations.FpolicyEventFileOperationsSchema", unknown=EXCLUDE, data_key="file_operations")
    r""" The file_operations field of the fpolicy_events. """

    filters = fields.Nested("netapp_ontap.models.fpolicy_event_filters.FpolicyEventFiltersSchema", unknown=EXCLUDE, data_key="filters")
    r""" The filters field of the fpolicy_events. """

    monitor_fileop_failure = fields.Boolean(data_key="monitor_fileop_failure")
    r""" Specifies whether failed file operations monitoring is required. """

    name = fields.Str(data_key="name")
    r""" Specifies the name of the FPolicy event.

Example: event_nfs_close """

    protocol = fields.Str(data_key="protocol")
    r""" Protocol for which event is created. If you specify protocol, then you
must also specify a valid value for the file operation parameters.
  The value of this parameter must be one of the following:

    * cifs  - for the CIFS protocol.
    * nfsv3 - for the NFSv3 protocol.
    * nfsv4 - for the NFSv4 protocol.


Valid choices:

* cifs
* nfsv3
* nfsv4 """

    volume_monitoring = fields.Boolean(data_key="volume_monitoring")
    r""" Specifies whether volume operation monitoring is required. """

    @property
    def resource(self):
        return FpolicyEvents

    gettable_fields = [
        "file_operations",
        "filters",
        "monitor_fileop_failure",
        "name",
        "protocol",
        "volume_monitoring",
    ]
    """file_operations,filters,monitor_fileop_failure,name,protocol,volume_monitoring,"""

    patchable_fields = [
        "file_operations",
        "filters",
        "monitor_fileop_failure",
        "protocol",
        "volume_monitoring",
    ]
    """file_operations,filters,monitor_fileop_failure,protocol,volume_monitoring,"""

    postable_fields = [
        "file_operations",
        "filters",
        "monitor_fileop_failure",
        "name",
        "protocol",
        "volume_monitoring",
    ]
    """file_operations,filters,monitor_fileop_failure,name,protocol,volume_monitoring,"""


class FpolicyEvents(Resource):

    _schema = FpolicyEventsSchema
