r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SvmMigrationVolumePlacementVolumeAggregatePairs", "SvmMigrationVolumePlacementVolumeAggregatePairsSchema"]
__pdoc__ = {
    "SvmMigrationVolumePlacementVolumeAggregatePairsSchema.resource": False,
    "SvmMigrationVolumePlacementVolumeAggregatePairsSchema.opts": False,
    "SvmMigrationVolumePlacementVolumeAggregatePairs": False,
}


class SvmMigrationVolumePlacementVolumeAggregatePairsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SvmMigrationVolumePlacementVolumeAggregatePairs object"""

    aggregate = fields.Nested("netapp_ontap.resources.aggregate.AggregateSchema", unknown=EXCLUDE, data_key="aggregate")
    r""" The aggregate field of the svm_migration_volume_placement_volume_aggregate_pairs. """

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", unknown=EXCLUDE, data_key="volume")
    r""" The volume field of the svm_migration_volume_placement_volume_aggregate_pairs. """

    @property
    def resource(self):
        return SvmMigrationVolumePlacementVolumeAggregatePairs

    gettable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,volume.name,volume.uuid,"""

    postable_fields = [
        "aggregate.links",
        "aggregate.name",
        "aggregate.uuid",
        "volume.name",
        "volume.uuid",
    ]
    """aggregate.links,aggregate.name,aggregate.uuid,volume.name,volume.uuid,"""


class SvmMigrationVolumePlacementVolumeAggregatePairs(Resource):

    _schema = SvmMigrationVolumePlacementVolumeAggregatePairsSchema
