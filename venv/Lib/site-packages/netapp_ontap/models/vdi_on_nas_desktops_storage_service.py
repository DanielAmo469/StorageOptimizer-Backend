r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VdiOnNasDesktopsStorageService", "VdiOnNasDesktopsStorageServiceSchema"]
__pdoc__ = {
    "VdiOnNasDesktopsStorageServiceSchema.resource": False,
    "VdiOnNasDesktopsStorageServiceSchema.opts": False,
    "VdiOnNasDesktopsStorageService": False,
}


class VdiOnNasDesktopsStorageServiceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VdiOnNasDesktopsStorageService object"""

    name = fields.Str(data_key="name")
    r""" The storage service of the desktops.

Valid choices:

* extreme
* performance
* value """

    @property
    def resource(self):
        return VdiOnNasDesktopsStorageService

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


class VdiOnNasDesktopsStorageService(Resource):

    _schema = VdiOnNasDesktopsStorageServiceSchema
