r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["TopMetricsSvmUserVolumes", "TopMetricsSvmUserVolumesSchema"]
__pdoc__ = {
    "TopMetricsSvmUserVolumesSchema.resource": False,
    "TopMetricsSvmUserVolumesSchema.opts": False,
    "TopMetricsSvmUserVolumes": False,
}


class TopMetricsSvmUserVolumesSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TopMetricsSvmUserVolumes object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the top_metrics_svm_user_volumes. """

    name = fields.Str(data_key="name")
    r""" The name of the volume.

Example: volume1 """

    uuid = fields.Str(data_key="uuid")
    r""" Unique identifier for the volume. This corresponds to the instance-uuid that is exposed in the CLI and ONTAPI. It does not change due to a volume move.

Example: 028baa66-41bd-11e9-81d5-00a0986138f7 """

    @property
    def resource(self):
        return TopMetricsSvmUserVolumes

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""

    postable_fields = [
        "name",
        "uuid",
    ]
    """name,uuid,"""


class TopMetricsSvmUserVolumes(Resource):

    _schema = TopMetricsSvmUserVolumesSchema
