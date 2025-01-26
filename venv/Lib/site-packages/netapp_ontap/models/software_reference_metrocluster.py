r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SoftwareReferenceMetrocluster", "SoftwareReferenceMetroclusterSchema"]
__pdoc__ = {
    "SoftwareReferenceMetroclusterSchema.resource": False,
    "SoftwareReferenceMetroclusterSchema.opts": False,
    "SoftwareReferenceMetrocluster": False,
}


class SoftwareReferenceMetroclusterSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SoftwareReferenceMetrocluster object"""

    clusters = fields.List(fields.Nested("netapp_ontap.models.software_mcc.SoftwareMccSchema", unknown=EXCLUDE), data_key="clusters")
    r""" List of MetroCluster sites, statuses, and active ONTAP versions. """

    progress_details = fields.Nested("netapp_ontap.models.software_reference_metrocluster_progress_details.SoftwareReferenceMetroclusterProgressDetailsSchema", unknown=EXCLUDE, data_key="progress_details")
    r""" The progress_details field of the software_reference_metrocluster. """

    progress_summary = fields.Nested("netapp_ontap.models.software_reference_metrocluster_progress_summary.SoftwareReferenceMetroclusterProgressSummarySchema", unknown=EXCLUDE, data_key="progress_summary")
    r""" The progress_summary field of the software_reference_metrocluster. """

    @property
    def resource(self):
        return SoftwareReferenceMetrocluster

    gettable_fields = [
        "clusters",
        "progress_details",
        "progress_summary",
    ]
    """clusters,progress_details,progress_summary,"""

    patchable_fields = [
        "progress_details",
        "progress_summary",
    ]
    """progress_details,progress_summary,"""

    postable_fields = [
        "progress_details",
        "progress_summary",
    ]
    """progress_details,progress_summary,"""


class SoftwareReferenceMetrocluster(Resource):

    _schema = SoftwareReferenceMetroclusterSchema
