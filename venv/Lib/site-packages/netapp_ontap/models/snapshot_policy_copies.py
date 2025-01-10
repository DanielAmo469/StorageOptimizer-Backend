r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapshotPolicyCopies", "SnapshotPolicyCopiesSchema"]
__pdoc__ = {
    "SnapshotPolicyCopiesSchema.resource": False,
    "SnapshotPolicyCopiesSchema.opts": False,
    "SnapshotPolicyCopies": False,
}


class SnapshotPolicyCopiesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapshotPolicyCopies object"""

    count = Size(data_key="count")
    r""" The number of Snapshot copies to maintain for this schedule. """

    prefix = fields.Str(data_key="prefix")
    r""" The prefix to use while creating Snapshot copies at regular intervals. """

    retention_period = fields.Str(data_key="retention_period")
    r""" The retention period of Snapshot copies for this schedule. The retention period value represents a duration and must be specified in the ISO-8601 duration format. The retention period can be in years, months, days, hours, and minutes. A period specified for years, months, and days is represented in the ISO-8601 format as "P<num>Y", "P<num>M", "P<num>D" respectively, for example "P10Y" represents a duration of 10 years. A duration in hours and minutes is represented by "PT<num>H" and "PT<num>M" respectively. The period string must contain only a single time element that is, either years, months, days, hours, or minutes. A duration which combines different periods is not supported, for example "P1Y10M" is not supported. """

    schedule = fields.Nested("netapp_ontap.models.snapshot_policy_copies_schedule.SnapshotPolicyCopiesScheduleSchema", unknown=EXCLUDE, data_key="schedule")
    r""" The schedule field of the snapshot_policy_copies. """

    snapmirror_label = fields.Str(data_key="snapmirror_label")
    r""" Label for SnapMirror operations """

    @property
    def resource(self):
        return SnapshotPolicyCopies

    gettable_fields = [
        "count",
        "prefix",
        "retention_period",
        "schedule",
        "snapmirror_label",
    ]
    """count,prefix,retention_period,schedule,snapmirror_label,"""

    patchable_fields = [
        "count",
        "prefix",
        "retention_period",
        "schedule",
        "snapmirror_label",
    ]
    """count,prefix,retention_period,schedule,snapmirror_label,"""

    postable_fields = [
        "count",
        "prefix",
        "retention_period",
        "schedule",
        "snapmirror_label",
    ]
    """count,prefix,retention_period,schedule,snapmirror_label,"""


class SnapshotPolicyCopies(Resource):

    _schema = SnapshotPolicyCopiesSchema
