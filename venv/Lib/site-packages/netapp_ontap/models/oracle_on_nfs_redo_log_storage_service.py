r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["OracleOnNfsRedoLogStorageService", "OracleOnNfsRedoLogStorageServiceSchema"]
__pdoc__ = {
    "OracleOnNfsRedoLogStorageServiceSchema.resource": False,
    "OracleOnNfsRedoLogStorageServiceSchema.opts": False,
    "OracleOnNfsRedoLogStorageService": False,
}


class OracleOnNfsRedoLogStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the OracleOnNfsRedoLogStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the redo log group.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return OracleOnNfsRedoLogStorageService

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


class OracleOnNfsRedoLogStorageService(Resource):

    _schema = OracleOnNfsRedoLogStorageServiceSchema
