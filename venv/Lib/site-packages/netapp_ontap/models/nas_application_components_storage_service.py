r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NasApplicationComponentsStorageService", "NasApplicationComponentsStorageServiceSchema"]
__pdoc__ = {
    "NasApplicationComponentsStorageServiceSchema.resource": False,
    "NasApplicationComponentsStorageServiceSchema.opts": False,
    "NasApplicationComponentsStorageService": False,
}


class NasApplicationComponentsStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NasApplicationComponentsStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the application component.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return NasApplicationComponentsStorageService

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


class NasApplicationComponentsStorageService(Resource):

    _schema = NasApplicationComponentsStorageServiceSchema
