r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["AntiRansomwareAttackReport", "AntiRansomwareAttackReportSchema"]
__pdoc__ = {
    "AntiRansomwareAttackReportSchema.resource": False,
    "AntiRansomwareAttackReportSchema.opts": False,
    "AntiRansomwareAttackReport": False,
}


class AntiRansomwareAttackReportSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the AntiRansomwareAttackReport object"""

    links = fields.Nested("netapp_ontap.models.anti_ransomware_attack_report_links.AntiRansomwareAttackReportLinksSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the anti_ransomware_attack_report. """

    time = ImpreciseDateTime(data_key="time")
    r""" Timestamp at which ransomware attack is observed.

Example: 2021-06-01T15:06:41.000+0000 """

    @property
    def resource(self):
        return AntiRansomwareAttackReport

    gettable_fields = [
        "links",
        "time",
    ]
    """links,time,"""

    patchable_fields = [
        "links",
        "time",
    ]
    """links,time,"""

    postable_fields = [
        "links",
        "time",
    ]
    """links,time,"""


class AntiRansomwareAttackReport(Resource):

    _schema = AntiRansomwareAttackReportSchema
