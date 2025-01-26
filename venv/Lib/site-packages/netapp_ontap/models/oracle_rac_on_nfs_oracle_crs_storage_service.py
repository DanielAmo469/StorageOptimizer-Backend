r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["OracleRacOnNfsOracleCrsStorageService", "OracleRacOnNfsOracleCrsStorageServiceSchema"]
__pdoc__ = {
    "OracleRacOnNfsOracleCrsStorageServiceSchema.resource": False,
    "OracleRacOnNfsOracleCrsStorageServiceSchema.opts": False,
    "OracleRacOnNfsOracleCrsStorageService": False,
}


class OracleRacOnNfsOracleCrsStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the OracleRacOnNfsOracleCrsStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the Oracle CRS volume.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return OracleRacOnNfsOracleCrsStorageService

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


class OracleRacOnNfsOracleCrsStorageService(Resource):

    _schema = OracleRacOnNfsOracleCrsStorageServiceSchema
