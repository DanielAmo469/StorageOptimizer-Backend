r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AggregateInodeAttributes", "AggregateInodeAttributesSchema"]
__pdoc__ = {
    "AggregateInodeAttributesSchema.resource": False,
    "AggregateInodeAttributesSchema.opts": False,
    "AggregateInodeAttributes": False,
}


class AggregateInodeAttributesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AggregateInodeAttributes object"""

    file_private_capacity = Size(data_key="file_private_capacity")
    r""" Number of files that can currently be stored on disk for system metadata files. This number will dynamically increase as more system files are created.
This is an advanced property; there is an added computationl cost to retrieving its value. The field is not populated for either a collection GET or an instance GET unless it is explicitly requested using the <i>fields</i> query parameter containing either footprint or **.


Example: 31136 """

    file_public_capacity = Size(data_key="file_public_capacity")
    r""" Number of files that can currently be stored on disk for user-visible files.  This number will dynamically increase as more user-visible files are created.
This is an advanced property; there is an added computational cost to retrieving its value. The field is not populated for either a collection GET or an instance GET unless it is explicitly requested using the <i>fields</i> query parameter containing either footprint or **.


Example: 31136 """

    files_private_used = Size(data_key="files_private_used")
    r""" Number of system metadata files used. If the referenced file system is restricted or offline, a value of 0 is returned.
This is an advanced property; there is an added computational cost to retrieving its value. The field is not populated for either a collection GET or an instance GET unless it is explicitly requested using the <i>fields</i> query parameter containing either footprint or **.


Example: 502 """

    files_total = Size(data_key="files_total")
    r""" Maximum number of user-visible files that this referenced file system can currently hold. If the referenced file system is restricted or offline, a value of 0 is returned.

Example: 31136 """

    files_used = Size(data_key="files_used")
    r""" Number of user-visible files used in the referenced file system. If the referenced file system is restricted or offline, a value of 0 is returned.

Example: 97 """

    max_files_available = Size(data_key="max_files_available")
    r""" The count of the maximum number of user-visible files currently allowable on the referenced file system.

Example: 31136 """

    max_files_possible = Size(data_key="max_files_possible")
    r""" The largest value to which the maxfiles-available parameter can be increased by reconfiguration, on the referenced file system.

Example: 2844525 """

    max_files_used = Size(data_key="max_files_used")
    r""" The number of user-visible files currently in use on the referenced file system.

Example: 97 """

    used_percent = Size(data_key="used_percent")
    r""" The percentage of disk space currently in use based on user-visible file count on the referenced file system.

Example: 5 """

    version = Size(data_key="version")
    r""" The inofile-version of the aggregate. If the referenced file system is restricted or offline, a value of 0 is returned.
This is an advanced property; there is an added computational cost to retrieving its value. The field is not populated for either a collection GET or an instance GET unless it is explicitly requested using the <i>fields</i> query parameter containing either footprint or **.


Example: 4 """

    @property
    def resource(self):
        return AggregateInodeAttributes

    gettable_fields = [
        "file_private_capacity",
        "file_public_capacity",
        "files_private_used",
        "files_total",
        "files_used",
        "max_files_available",
        "max_files_possible",
        "max_files_used",
        "used_percent",
        "version",
    ]
    """file_private_capacity,file_public_capacity,files_private_used,files_total,files_used,max_files_available,max_files_possible,max_files_used,used_percent,version,"""

    patchable_fields = [
        "file_private_capacity",
        "file_public_capacity",
        "files_private_used",
        "files_total",
        "files_used",
        "max_files_available",
        "max_files_possible",
        "max_files_used",
        "used_percent",
        "version",
    ]
    """file_private_capacity,file_public_capacity,files_private_used,files_total,files_used,max_files_available,max_files_possible,max_files_used,used_percent,version,"""

    postable_fields = [
        "file_private_capacity",
        "file_public_capacity",
        "files_private_used",
        "files_total",
        "files_used",
        "max_files_available",
        "max_files_possible",
        "max_files_used",
        "used_percent",
        "version",
    ]
    """file_private_capacity,file_public_capacity,files_private_used,files_total,files_used,max_files_available,max_files_possible,max_files_used,used_percent,version,"""


class AggregateInodeAttributes(Resource):

    _schema = AggregateInodeAttributesSchema
