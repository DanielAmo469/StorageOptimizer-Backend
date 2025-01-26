r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapshotPolicyCopiesSchedule", "SnapshotPolicyCopiesScheduleSchema"]
__pdoc__ = {
    "SnapshotPolicyCopiesScheduleSchema.resource": False,
    "SnapshotPolicyCopiesScheduleSchema.opts": False,
    "SnapshotPolicyCopiesSchedule": False,
}


class SnapshotPolicyCopiesScheduleSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapshotPolicyCopiesSchedule object"""

    name = fields.Str(data_key="name")
    r""" Schedule at which Snapshot copies are captured on the volume. Some common schedules already defined in the system are hourly, daily, weekly, at 15 minute intervals, and at 5 minute intervals. Snapshot copy policies with custom schedules can be referenced.

Example: hourly """

    @property
    def resource(self):
        return SnapshotPolicyCopiesSchedule

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class SnapshotPolicyCopiesSchedule(Resource):

    _schema = SnapshotPolicyCopiesScheduleSchema
