r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationStatisticsComponentsIops", "ApplicationStatisticsComponentsIopsSchema"]
__pdoc__ = {
    "ApplicationStatisticsComponentsIopsSchema.resource": False,
    "ApplicationStatisticsComponentsIopsSchema.opts": False,
    "ApplicationStatisticsComponentsIops": False,
}


class ApplicationStatisticsComponentsIopsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationStatisticsComponentsIops object"""

    per_tb = Size(data_key="per_tb")
    r""" The number of IOPS per terabyte of logical space currently being used by the application component. """

    total = Size(data_key="total")
    r""" The total number of IOPS being used by the application component. """

    @property
    def resource(self):
        return ApplicationStatisticsComponentsIops

    gettable_fields = [
        "per_tb",
        "total",
    ]
    """per_tb,total,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ApplicationStatisticsComponentsIops(Resource):

    _schema = ApplicationStatisticsComponentsIopsSchema
