r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsSvmClientExcludedVolume", "TopMetricsSvmClientExcludedVolumeSchema"]
__pdoc__ = {
    "TopMetricsSvmClientExcludedVolumeSchema.resource": False,
    "TopMetricsSvmClientExcludedVolumeSchema.opts": False,
    "TopMetricsSvmClientExcludedVolume": False,
}


class TopMetricsSvmClientExcludedVolumeSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsSvmClientExcludedVolume object"""

    reason = fields.Nested("netapp_ontap.models.top_metrics_svm_client_excluded_volume_reason.TopMetricsSvmClientExcludedVolumeReasonSchema", unknown=EXCLUDE, data_key="reason")
    r""" The reason field of the top_metrics_svm_client_excluded_volume. """

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", unknown=EXCLUDE, data_key="volume")
    r""" The volume field of the top_metrics_svm_client_excluded_volume. """

    @property
    def resource(self):
        return TopMetricsSvmClientExcludedVolume

    gettable_fields = [
        "reason",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """reason,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class TopMetricsSvmClientExcludedVolume(Resource):

    _schema = TopMetricsSvmClientExcludedVolumeSchema
