r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SelfLink", "SelfLinkSchema"]
__pdoc__ = {
    "SelfLinkSchema.resource": False,
    "SelfLinkSchema.opts": False,
    "SelfLink": False,
}


class SelfLinkSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SelfLink object"""

    self_ = fields.Nested("netapp_ontap.models.href.HrefSchema", unknown=EXCLUDE, data_key="self")
    r""" The self_ field of the self_link. """

    @property
    def resource(self):
        return SelfLink

    gettable_fields = [
        "self_",
    ]
    """self_,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class SelfLink(Resource):

    _schema = SelfLinkSchema
