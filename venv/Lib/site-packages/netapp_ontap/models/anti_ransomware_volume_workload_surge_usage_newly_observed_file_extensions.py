r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensions", "AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensionsSchema"]
__pdoc__ = {
    "AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensionsSchema.resource": False,
    "AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensionsSchema.opts": False,
    "AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensions": False,
}


class AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensionsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensions object"""

    count = Size(data_key="count")
    r""" Count of newly observed file extensions.

Example: """

    name = fields.Str(data_key="name")
    r""" Name of the newly observed file extension.

Example: """

    @property
    def resource(self):
        return AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensions

    gettable_fields = [
        "count",
        "name",
    ]
    """count,name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensions(Resource):

    _schema = AntiRansomwareVolumeWorkloadSurgeUsageNewlyObservedFileExtensionsSchema
