r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupConsistencyGroupsVolumesNasCifs", "ConsistencyGroupConsistencyGroupsVolumesNasCifsSchema"]
__pdoc__ = {
    "ConsistencyGroupConsistencyGroupsVolumesNasCifsSchema.resource": False,
    "ConsistencyGroupConsistencyGroupsVolumesNasCifsSchema.opts": False,
    "ConsistencyGroupConsistencyGroupsVolumesNasCifs": False,
}


class ConsistencyGroupConsistencyGroupsVolumesNasCifsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupConsistencyGroupsVolumesNasCifs object"""

    shares = fields.List(fields.Nested("netapp_ontap.models.consistency_group_cifs_share.ConsistencyGroupCifsShareSchema", unknown=EXCLUDE), data_key="shares")
    r""" The shares field of the consistency_group_consistency_groups_volumes_nas_cifs. """

    @property
    def resource(self):
        return ConsistencyGroupConsistencyGroupsVolumesNasCifs

    gettable_fields = [
        "shares",
    ]
    """shares,"""

    patchable_fields = [
        "shares",
    ]
    """shares,"""

    postable_fields = [
        "shares",
    ]
    """shares,"""


class ConsistencyGroupConsistencyGroupsVolumesNasCifs(Resource):

    _schema = ConsistencyGroupConsistencyGroupsVolumesNasCifsSchema
