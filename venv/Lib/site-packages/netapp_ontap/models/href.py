r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["Href", "HrefSchema"]
__pdoc__ = {
    "HrefSchema.resource": False,
    "HrefSchema.opts": False,
    "Href": False,
}


class HrefSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Href object"""

    href = fields.Str(data_key="href")
    r""" The href field of the href.

Example: /api/resourcelink """

    @property
    def resource(self):
        return Href

    gettable_fields = [
        "href",
    ]
    """href,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class Href(Resource):

    _schema = HrefSchema
