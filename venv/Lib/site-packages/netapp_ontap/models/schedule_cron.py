r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ScheduleCron", "ScheduleCronSchema"]
__pdoc__ = {
    "ScheduleCronSchema.resource": False,
    "ScheduleCronSchema.opts": False,
    "ScheduleCron": False,
}


class ScheduleCronSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ScheduleCron object"""

    days = fields.List(Size, data_key="days")
    r""" The days of the month the schedule runs. Leave empty for all. """

    hours = fields.List(Size, data_key="hours")
    r""" The hours of the day the schedule runs. Leave empty for all. """

    minutes = fields.List(Size, data_key="minutes")
    r""" The minutes the schedule runs. Required on POST for a cron schedule. """

    months = fields.List(Size, data_key="months")
    r""" The months of the year the schedule runs. Leave empty for all. """

    weekdays = fields.List(Size, data_key="weekdays")
    r""" The weekdays the schedule runs. Leave empty for all. """

    @property
    def resource(self):
        return ScheduleCron

    gettable_fields = [
        "days",
        "hours",
        "minutes",
        "months",
        "weekdays",
    ]
    """days,hours,minutes,months,weekdays,"""

    patchable_fields = [
        "days",
        "hours",
        "minutes",
        "months",
        "weekdays",
    ]
    """days,hours,minutes,months,weekdays,"""

    postable_fields = [
        "days",
        "hours",
        "minutes",
        "months",
        "weekdays",
    ]
    """days,hours,minutes,months,weekdays,"""


class ScheduleCron(Resource):

    _schema = ScheduleCronSchema
