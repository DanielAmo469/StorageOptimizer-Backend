r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["OracleOnNfsArchiveLogStorageService", "OracleOnNfsArchiveLogStorageServiceSchema"]
__pdoc__ = {
    "OracleOnNfsArchiveLogStorageServiceSchema.resource": False,
    "OracleOnNfsArchiveLogStorageServiceSchema.opts": False,
    "OracleOnNfsArchiveLogStorageService": False,
}


class OracleOnNfsArchiveLogStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the OracleOnNfsArchiveLogStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the archive log.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return OracleOnNfsArchiveLogStorageService

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class OracleOnNfsArchiveLogStorageService(Resource):

    _schema = OracleOnNfsArchiveLogStorageServiceSchema
