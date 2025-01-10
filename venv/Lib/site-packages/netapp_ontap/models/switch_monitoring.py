r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SwitchMonitoring", "SwitchMonitoringSchema"]
__pdoc__ = {
    "SwitchMonitoringSchema.resource": False,
    "SwitchMonitoringSchema.opts": False,
    "SwitchMonitoring": False,
}


class SwitchMonitoringSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SwitchMonitoring object"""

    enabled = fields.Boolean(data_key="enabled")
    r""" Enable Health Monitoring. """

    monitored = fields.Boolean(data_key="monitored")
    r""" Is Monitored. """

    reason = fields.Str(data_key="reason")
    r""" Reason For Not Monitoring.

Valid choices:

* none
* unsupported_model
* user_deleted
* bad_ip_address
* invalid_snmp_settings
* bad_model
* invalid_software_version
* user_disabled
* unknown """

    @property
    def resource(self):
        return SwitchMonitoring

    gettable_fields = [
        "enabled",
        "monitored",
        "reason",
    ]
    """enabled,monitored,reason,"""

    patchable_fields = [
        "enabled",
    ]
    """enabled,"""

    postable_fields = [
        "enabled",
    ]
    """enabled,"""


class SwitchMonitoring(Resource):

    _schema = SwitchMonitoringSchema
