r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationStatisticsLatency", "ApplicationStatisticsLatencySchema"]
__pdoc__ = {
    "ApplicationStatisticsLatencySchema.resource": False,
    "ApplicationStatisticsLatencySchema.opts": False,
    "ApplicationStatisticsLatency": False,
}


class ApplicationStatisticsLatencySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationStatisticsLatency object"""

    average = Size(data_key="average")
    r""" The cumulative average response time in microseconds for this application. """

    raw = Size(data_key="raw")
    r""" The cumulative response time in microseconds for this application. """

    @property
    def resource(self):
        return ApplicationStatisticsLatency

    gettable_fields = [
        "average",
        "raw",
    ]
    """average,raw,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationStatisticsLatency(Resource):

    _schema = ApplicationStatisticsLatencySchema
