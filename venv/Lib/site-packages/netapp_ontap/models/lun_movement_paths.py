r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunMovementPaths", "LunMovementPathsSchema"]
__pdoc__ = {
    "LunMovementPathsSchema.resource": False,
    "LunMovementPathsSchema.opts": False,
    "LunMovementPaths": False,
}


class LunMovementPathsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunMovementPaths object"""

    destination = fields.Str(data_key="destination")
    r""" The fully qualified path of the LUN movement destination composed of a "/vol" prefix, the volume name, the (optional) qtree name, and base name of the LUN.


Example: /vol/vol1/lun1 """

    source = fields.Str(data_key="source")
    r""" The fully qualified path of the LUN movement source composed of a "/vol" prefix, the volume name, the (optional) qtree name, and base name of the LUN.


Example: /vol/vol2/lun2 """

    @property
    def resource(self):
        return LunMovementPaths

    gettable_fields = [
        "destination",
        "source",
    ]
    """destination,source,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class LunMovementPaths(Resource):

    _schema = LunMovementPathsSchema
