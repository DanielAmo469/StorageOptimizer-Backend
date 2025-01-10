r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SnapmirrorEndpoint", "SnapmirrorEndpointSchema"]
__pdoc__ = {
    "SnapmirrorEndpointSchema.resource": False,
    "SnapmirrorEndpointSchema.opts": False,
    "SnapmirrorEndpoint": False,
}


class SnapmirrorEndpointSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SnapmirrorEndpoint object"""

    cluster = fields.Nested("netapp_ontap.resources.cluster.ClusterSchema", unknown=EXCLUDE, data_key="cluster")
    r""" The cluster field of the snapmirror_endpoint. """

    consistency_group_volumes = fields.List(fields.Nested("netapp_ontap.models.snapmirror_endpoint_consistency_group_volumes.SnapmirrorEndpointConsistencyGroupVolumesSchema", unknown=EXCLUDE), data_key="consistency_group_volumes")
    r""" Mandatory property for a Consistency Group endpoint. Specifies the list of FlexVol volumes for a Consistency Group. """

    ipspace = fields.Str(data_key="ipspace")
    r""" Optional property to specify the IPSpace of the SVM.

Example: Default """

    path = fields.Str(data_key="path")
    r""" ONTAP FlexVol/FlexGroup - svm1:volume1
ONTAP SVM               - svm1:
ONTAP Consistency Group - svm1:/cg/cg_name
ONTAP S3                - svm1:/bucket/bucket1
NON-ONTAP               - objstore1:/objstore


Example: svm1:volume1 """

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", unknown=EXCLUDE, data_key="svm")
    r""" The svm field of the snapmirror_endpoint. """

    @property
    def resource(self):
        return SnapmirrorEndpoint

    gettable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "consistency_group_volumes",
        "path",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,consistency_group_volumes,path,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "path",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,path,svm.name,svm.uuid,"""

    postable_fields = [
        "cluster.links",
        "cluster.name",
        "cluster.uuid",
        "consistency_group_volumes",
        "ipspace",
        "path",
        "svm.name",
        "svm.uuid",
    ]
    """cluster.links,cluster.name,cluster.uuid,consistency_group_volumes,ipspace,path,svm.name,svm.uuid,"""


class SnapmirrorEndpoint(Resource):

    _schema = SnapmirrorEndpointSchema
