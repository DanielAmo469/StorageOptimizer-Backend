r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventLinks", "EmsEventLinksSchema"]
__pdoc__ = {
    "EmsEventLinksSchema.resource": False,
    "EmsEventLinksSchema.opts": False,
    "EmsEventLinks": False,
}


class EmsEventLinksSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventLinks object"""

    self_ = fields.Nested("netapp_ontap.models.href.HrefSchema", unknown=EXCLUDE, data_key="self")
    r""" The self_ field of the ems_event_links. """

    @property
    def resource(self):
        return EmsEventLinks

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


class EmsEventLinks(Resource):

    _schema = EmsEventLinksSchema
