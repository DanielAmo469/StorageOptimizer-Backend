r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareVolumeWorkloadTypicalUsage", "AntiRansomwareVolumeWorkloadTypicalUsageSchema"]
__pdoc__ = {
    "AntiRansomwareVolumeWorkloadTypicalUsageSchema.resource": False,
    "AntiRansomwareVolumeWorkloadTypicalUsageSchema.opts": False,
    "AntiRansomwareVolumeWorkloadTypicalUsage": False,
}


class AntiRansomwareVolumeWorkloadTypicalUsageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareVolumeWorkloadTypicalUsage object"""

    file_create_peak_rate_per_minute = Size(data_key="file_create_peak_rate_per_minute")
    r""" Typical peak rate of file creates per minute in the workload of the volume.

Example: 50 """

    file_delete_peak_rate_per_minute = Size(data_key="file_delete_peak_rate_per_minute")
    r""" Typical peak rate of file deletes per minute in the workload of the volume.

Example: 10 """

    file_rename_peak_rate_per_minute = Size(data_key="file_rename_peak_rate_per_minute")
    r""" Typical peak rate of file renames per minute in the workload of the volume.

Example: 5 """

    high_entropy_data_write_peak_percent = Size(data_key="high_entropy_data_write_peak_percent")
    r""" Typical peak percentage of high entropy data writes in the volume.

Example: 10 """

    high_entropy_data_write_peak_rate_kb_per_minute = Size(data_key="high_entropy_data_write_peak_rate_kb_per_minute")
    r""" Typical peak high entropy data write rate in the volume, in KBs per minute.

Example: 1200 """

    @property
    def resource(self):
        return AntiRansomwareVolumeWorkloadTypicalUsage

    gettable_fields = [
        "file_create_peak_rate_per_minute",
        "file_delete_peak_rate_per_minute",
        "file_rename_peak_rate_per_minute",
        "high_entropy_data_write_peak_percent",
        "high_entropy_data_write_peak_rate_kb_per_minute",
    ]
    """file_create_peak_rate_per_minute,file_delete_peak_rate_per_minute,file_rename_peak_rate_per_minute,high_entropy_data_write_peak_percent,high_entropy_data_write_peak_rate_kb_per_minute,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AntiRansomwareVolumeWorkloadTypicalUsage(Resource):

    _schema = AntiRansomwareVolumeWorkloadTypicalUsageSchema
