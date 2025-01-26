r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationStatistics", "ApplicationStatisticsSchema"]
__pdoc__ = {
    "ApplicationStatisticsSchema.resource": False,
    "ApplicationStatisticsSchema.opts": False,
    "ApplicationStatistics": False,
}


class ApplicationStatisticsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationStatistics object"""

    components = fields.List(fields.Nested("netapp_ontap.models.application_statistics_components.ApplicationStatisticsComponentsSchema", unknown=EXCLUDE), data_key="components")
    r""" The components field of the application_statistics. """

    iops = fields.Nested("netapp_ontap.models.application_statistics_iops.ApplicationStatisticsIopsSchema", unknown=EXCLUDE, data_key="iops")
    r""" The iops field of the application_statistics. """

    latency = fields.Nested("netapp_ontap.models.application_statistics_latency.ApplicationStatisticsLatencySchema", unknown=EXCLUDE, data_key="latency")
    r""" The latency field of the application_statistics. """

    shared_storage_pool = fields.Boolean(data_key="shared_storage_pool")
    r""" An application is considered to use a shared storage pool if storage elements for multiple components reside on the same aggregate. """

    snapshot = fields.Nested("netapp_ontap.models.application_statistics_components_snapshot.ApplicationStatisticsComponentsSnapshotSchema", unknown=EXCLUDE, data_key="snapshot")
    r""" The snapshot field of the application_statistics. """

    space = fields.Nested("netapp_ontap.models.application_statistics_space.ApplicationStatisticsSpaceSchema", unknown=EXCLUDE, data_key="space")
    r""" The space field of the application_statistics. """

    statistics_incomplete = fields.Boolean(data_key="statistics_incomplete")
    r""" If not all storage elements of the application are currently available, the returned statistics might only include data from those elements that were available. """

    @property
    def resource(self):
        return ApplicationStatistics

    gettable_fields = [
        "components",
        "iops",
        "latency",
        "shared_storage_pool",
        "snapshot",
        "space",
        "statistics_incomplete",
    ]
    """components,iops,latency,shared_storage_pool,snapshot,space,statistics_incomplete,"""

    patchable_fields = [
        "iops",
        "latency",
        "snapshot",
        "space",
    ]
    """iops,latency,snapshot,space,"""

    postable_fields = [
        "iops",
        "latency",
        "snapshot",
        "space",
    ]
    """iops,latency,snapshot,space,"""


class ApplicationStatistics(Resource):

    _schema = ApplicationStatisticsSchema
