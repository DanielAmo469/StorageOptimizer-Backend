r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsOpenFileShare", "CifsOpenFileShareSchema"]
__pdoc__ = {
    "CifsOpenFileShareSchema.resource": False,
    "CifsOpenFileShareSchema.opts": False,
    "CifsOpenFileShare": False,
}


class CifsOpenFileShareSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsOpenFileShare object"""

    mode = fields.Str(data_key="mode")
    r""" The share mode used to open the file.
The share mode can be a combination of:
  - r: read mode
  - w: write mode
  - d: delete


Valid choices:

* r
* w
* d """

    name = fields.Str(data_key="name")
    r""" CIFS share name where the file resides.

Example: share1 """

    @property
    def resource(self):
        return CifsOpenFileShare

    gettable_fields = [
        "mode",
        "name",
    ]
    """mode,name,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class CifsOpenFileShare(Resource):

    _schema = CifsOpenFileShareSchema
