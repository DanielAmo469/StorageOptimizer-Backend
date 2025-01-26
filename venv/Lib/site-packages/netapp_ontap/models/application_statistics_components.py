r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationStatisticsComponents", "ApplicationStatisticsComponentsSchema"]
__pdoc__ = {
    "ApplicationStatisticsComponentsSchema.resource": False,
    "ApplicationStatisticsComponentsSchema.opts": False,
    "ApplicationStatisticsComponents": False,
}


class ApplicationStatisticsComponentsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationStatisticsComponents object"""

    iops = fields.Nested("netapp_ontap.models.application_statistics_components_iops.ApplicationStatisticsComponentsIopsSchema", unknown=EXCLUDE, data_key="iops")
    r""" The iops field of the application_statistics_components. """

    latency = fields.Nested("netapp_ontap.models.application_statistics_components_latency.ApplicationStatisticsComponentsLatencySchema", unknown=EXCLUDE, data_key="latency")
    r""" The latency field of the application_statistics_components. """

    name = fields.Str(data_key="name")
    r""" Component Name. """

    shared_storage_pool = fields.Boolean(data_key="shared_storage_pool")
    r""" An application component is considered to use a shared storage pool if storage elements for for other components reside on the same aggregate as storage elements for this component. """

    snapshot = fields.Nested("netapp_ontap.models.application_statistics_components_snapshot.ApplicationStatisticsComponentsSnapshotSchema", unknown=EXCLUDE, data_key="snapshot")
    r""" The snapshot field of the application_statistics_components. """

    space = fields.Nested("netapp_ontap.models.application_statistics_components_space.ApplicationStatisticsComponentsSpaceSchema", unknown=EXCLUDE, data_key="space")
    r""" The space field of the application_statistics_components. """

    statistics_incomplete = fields.Boolean(data_key="statistics_incomplete")
    r""" If not all storage elements of the application component are currently available, the returned statistics might only include data from those elements that were available. """

    storage_service = fields.Nested("netapp_ontap.models.application_statistics_components_storage_service.ApplicationStatisticsComponentsStorageServiceSchema", unknown=EXCLUDE, data_key="storage_service")
    r""" The storage_service field of the application_statistics_components. """

    uuid = fields.Str(data_key="uuid")
    r""" Component UUID. """

    @property
    def resource(self):
        return ApplicationStatisticsComponents

    gettable_fields = [
        "iops",
        "latency",
        "name",
        "shared_storage_pool",
        "snapshot",
        "space",
        "statistics_incomplete",
        "storage_service",
        "uuid",
    ]
    """iops,latency,name,shared_storage_pool,snapshot,space,statistics_incomplete,storage_service,uuid,"""

    patchable_fields = [
        "iops",
        "latency",
        "snapshot",
        "space",
        "storage_service",
    ]
    """iops,latency,snapshot,space,storage_service,"""

    postable_fields = [
        "iops",
        "latency",
        "snapshot",
        "space",
        "storage_service",
    ]
    """iops,latency,snapshot,space,storage_service,"""


class ApplicationStatisticsComponents(Resource):

    _schema = ApplicationStatisticsComponentsSchema
