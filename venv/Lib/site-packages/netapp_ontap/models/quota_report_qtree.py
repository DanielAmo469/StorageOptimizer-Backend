r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["QuotaReportQtree", "QuotaReportQtreeSchema"]
__pdoc__ = {
    "QuotaReportQtreeSchema.resource": False,
    "QuotaReportQtreeSchema.opts": False,
    "QuotaReportQtree": False,
}


class QuotaReportQtreeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the QuotaReportQtree object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the quota_report_qtree. """

    id = Size(data_key="id")
    r""" The unique identifier for a qtree.

Example: 1 """

    name = fields.Str(data_key="name")
    r""" The name of the qtree.

Example: qt1 """

    @property
    def resource(self):
        return QuotaReportQtree

    gettable_fields = [
        "links",
        "id",
        "name",
    ]
    """links,id,name,"""

    patchable_fields = [
        "id",
        "name",
    ]
    """id,name,"""

    postable_fields = [
        "id",
        "name",
    ]
    """id,name,"""


class QuotaReportQtree(Resource):

    _schema = QuotaReportQtreeSchema
