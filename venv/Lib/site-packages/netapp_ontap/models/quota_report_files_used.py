r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["QuotaReportFilesUsed", "QuotaReportFilesUsedSchema"]
__pdoc__ = {
    "QuotaReportFilesUsedSchema.resource": False,
    "QuotaReportFilesUsedSchema.opts": False,
    "QuotaReportFilesUsed": False,
}


class QuotaReportFilesUsedSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the QuotaReportFilesUsed object"""

    hard_limit_percent = Size(data_key="hard_limit_percent")
    r""" Total files used as a percentage of file hard limit """

    soft_limit_percent = Size(data_key="soft_limit_percent")
    r""" Total files used as a percentage of file soft limit """

    total = Size(data_key="total")
    r""" Total files used """

    @property
    def resource(self):
        return QuotaReportFilesUsed

    gettable_fields = [
        "hard_limit_percent",
        "soft_limit_percent",
        "total",
    ]
    """hard_limit_percent,soft_limit_percent,total,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class QuotaReportFilesUsed(Resource):

    _schema = QuotaReportFilesUsedSchema
