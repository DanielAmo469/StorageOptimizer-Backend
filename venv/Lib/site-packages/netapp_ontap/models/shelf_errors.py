r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ShelfErrors", "ShelfErrorsSchema"]
__pdoc__ = {
    "ShelfErrorsSchema.resource": False,
    "ShelfErrorsSchema.opts": False,
    "ShelfErrors": False,
}


class ShelfErrorsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ShelfErrors object"""

    reason = fields.Nested("netapp_ontap.models.error.ErrorSchema", unknown=EXCLUDE, data_key="reason")
    r""" The reason field of the shelf_errors. """

    @property
    def resource(self):
        return ShelfErrors

    gettable_fields = [
        "reason",
    ]
    """reason,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ShelfErrors(Resource):

    _schema = ShelfErrorsSchema
