r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareVolumeWorkloadSurgeUsage", "AntiRansomwareVolumeWorkloadSurgeUsageSchema"]
__pdoc__ = {
    "AntiRansomwareVolumeWorkloadSurgeUsageSchema.resource": False,
    "AntiRansomwareVolumeWorkloadSurgeUsageSchema.opts": False,
    "AntiRansomwareVolumeWorkloadSurgeUsage": False,
}


class AntiRansomwareVolumeWorkloadSurgeUsageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareVolumeWorkloadSurgeUsage object"""

    file_create_peak_rate_per_minute = Size(data_key="file_create_peak_rate_per_minute")
    r""" Peak rate of file creates per minute in the workload of the volume during surge.

Example: 10 """

    file_delete_peak_rate_per_minute = Size(data_key="file_delete_peak_rate_per_minute")
    r""" Peak rate of file deletes per minute in the workload of the volume during surge.

Example: 50 """

    file_rename_peak_rate_per_minute = Size(data_key="file_rename_peak_rate_per_minute")
    r""" Peak rate of file renames per minute in the workload of the volume during surge.

Example: 30 """

    high_entropy_data_write_peak_percent = Size(data_key="high_entropy_data_write_peak_percent")
    r""" Peak percentage of high entropy data writes in the volume during surge.

Example: 30 """

    high_entropy_data_write_peak_rate_kb_per_minute = Size(data_key="high_entropy_data_write_peak_rate_kb_per_minute")
    r""" Peak high entropy data write rate in the volume during surge, in KBs per minute.

Example: 2500 """

    newly_observed_file_extensions = fields.List(fields.Nested("netapp_ontap.models.anti_ransomware_volume_workload_surge_usage_newly_observed_file_extensions.AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensionsSchema", unknown=EXCLUDE), data_key="newly_observed_file_extensions")
    r""" New file extensions observed in the volume during surge. """

    time = ImpreciseDateTime(data_key="time")
    r""" Timestamp at which the first surge in the volume's workload is observed.

Example: 2021-12-01T17:46:20.000+0000 """

    @property
    def resource(self):
        return AntiRansomwareVolumeWorkloadSurgeUsage

    gettable_fields = [
        "file_create_peak_rate_per_minute",
        "file_delete_peak_rate_per_minute",
        "file_rename_peak_rate_per_minute",
        "high_entropy_data_write_peak_percent",
        "high_entropy_data_write_peak_rate_kb_per_minute",
        "newly_observed_file_extensions",
        "time",
    ]
    """file_create_peak_rate_per_minute,file_delete_peak_rate_per_minute,file_rename_peak_rate_per_minute,high_entropy_data_write_peak_percent,high_entropy_data_write_peak_rate_kb_per_minute,newly_observed_file_extensions,time,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AntiRansomwareVolumeWorkloadSurgeUsage(Resource):

    _schema = AntiRansomwareVolumeWorkloadSurgeUsageSchema
