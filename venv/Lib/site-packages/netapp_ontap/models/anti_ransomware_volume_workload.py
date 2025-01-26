r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareVolumeWorkload", "AntiRansomwareVolumeWorkloadSchema"]
__pdoc__ = {
    "AntiRansomwareVolumeWorkloadSchema.resource": False,
    "AntiRansomwareVolumeWorkloadSchema.opts": False,
    "AntiRansomwareVolumeWorkload": False,
}


class AntiRansomwareVolumeWorkloadSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareVolumeWorkload object"""

    file_extension_types_count = Size(data_key="file_extension_types_count")
    r""" Count of types of file extensions observed in the volume.

Example: 3 """

    file_extensions_observed = fields.List(fields.Str, data_key="file_extensions_observed")
    r""" File extensions observed in the volume.

Example: ["pdf","jpeg","txt"] """

    surge_usage = fields.Nested("netapp_ontap.models.anti_ransomware_volume_workload_surge_usage.AntiRansomwareVolumeWorkloadSurgeUsageSchema", unknown=EXCLUDE, data_key="surge_usage")
    r""" The surge_usage field of the anti_ransomware_volume_workload. """

    typical_usage = fields.Nested("netapp_ontap.models.anti_ransomware_volume_workload_typical_usage.AntiRansomwareVolumeWorkloadTypicalUsageSchema", unknown=EXCLUDE, data_key="typical_usage")
    r""" The typical_usage field of the anti_ransomware_volume_workload. """

    @property
    def resource(self):
        return AntiRansomwareVolumeWorkload

    gettable_fields = [
        "file_extension_types_count",
        "file_extensions_observed",
        "surge_usage",
        "typical_usage",
    ]
    """file_extension_types_count,file_extensions_observed,surge_usage,typical_usage,"""

    patchable_fields = [
        "surge_usage",
        "typical_usage",
    ]
    """surge_usage,typical_usage,"""

    postable_fields = [
        "surge_usage",
        "typical_usage",
    ]
    """surge_usage,typical_usage,"""


class AntiRansomwareVolumeWorkload(Resource):

    _schema = AntiRansomwareVolumeWorkloadSchema
