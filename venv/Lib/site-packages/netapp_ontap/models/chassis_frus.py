r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ChassisFrus", "ChassisFrusSchema"]
__pdoc__ = {
    "ChassisFrusSchema.resource": False,
    "ChassisFrusSchema.opts": False,
    "ChassisFrus": False,
}


class ChassisFrusSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ChassisFrus object"""

    id = fields.Str(data_key="id")
    r""" The id field of the chassis_frus. """

    state = fields.Str(data_key="state")
    r""" The state field of the chassis_frus.

Valid choices:

* ok
* error """

    type = fields.Str(data_key="type")
    r""" The type field of the chassis_frus.

Valid choices:

* fan
* psu """

    @property
    def resource(self):
        return ChassisFrus

    gettable_fields = [
        "id",
        "state",
        "type",
    ]
    """id,state,type,"""

    patchable_fields = [
        "id",
        "state",
        "type",
    ]
    """id,state,type,"""

    postable_fields = [
        "id",
        "state",
        "type",
    ]
    """id,state,type,"""


class ChassisFrus(Resource):

    _schema = ChassisFrusSchema
