r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["OracleOnNfsOraHomeStorageService", "OracleOnNfsOraHomeStorageServiceSchema"]
__pdoc__ = {
    "OracleOnNfsOraHomeStorageServiceSchema.resource": False,
    "OracleOnNfsOraHomeStorageServiceSchema.opts": False,
    "OracleOnNfsOraHomeStorageService": False,
}


class OracleOnNfsOraHomeStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the OracleOnNfsOraHomeStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the ORACLE_HOME storage volume.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return OracleOnNfsOraHomeStorageService

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


class OracleOnNfsOraHomeStorageService(Resource):

    _schema = OracleOnNfsOraHomeStorageServiceSchema
