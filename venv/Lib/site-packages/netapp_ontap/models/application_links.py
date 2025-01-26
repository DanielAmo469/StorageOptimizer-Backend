r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationLinks", "ApplicationLinksSchema"]
__pdoc__ = {
    "ApplicationLinksSchema.resource": False,
    "ApplicationLinksSchema.opts": False,
    "ApplicationLinks": False,
}


class ApplicationLinksSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationLinks object"""

    self_ = fields.Nested("netapp_ontap.models.href.HrefSchema", unknown=EXCLUDE, data_key="self")
    r""" The self_ field of the application_links. """

    snapshots = fields.Nested("netapp_ontap.models.href.HrefSchema", unknown=EXCLUDE, data_key="snapshots")
    r""" The snapshots field of the application_links. """

    @property
    def resource(self):
        return ApplicationLinks

    gettable_fields = [
        "self_",
        "snapshots",
    ]
    """self_,snapshots,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationLinks(Resource):

    _schema = ApplicationLinksSchema
