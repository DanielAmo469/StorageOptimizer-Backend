r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesExternalCache", "ClusterNodesExternalCacheSchema"]
__pdoc__ = {
    "ClusterNodesExternalCacheSchema.resource": False,
    "ClusterNodesExternalCacheSchema.opts": False,
    "ClusterNodesExternalCache": False,
}


class ClusterNodesExternalCacheSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesExternalCache object"""

    is_enabled = fields.Boolean(data_key="is_enabled")
    r""" Indicates whether the external cache is enabled.

Example: true """

    is_hya_enabled = fields.Boolean(data_key="is_hya_enabled")
    r""" Indicates whether HyA caching is enabled.

Example: true """

    is_rewarm_enabled = fields.Boolean(data_key="is_rewarm_enabled")
    r""" Indicates whether rewarm is enabled.

Example: true """

    pcs_size = Size(data_key="pcs_size")
    r""" PCS size in gigabytes. """

    @property
    def resource(self):
        return ClusterNodesExternalCache

    gettable_fields = [
        "is_enabled",
        "is_hya_enabled",
        "is_rewarm_enabled",
        "pcs_size",
    ]
    """is_enabled,is_hya_enabled,is_rewarm_enabled,pcs_size,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesExternalCache(Resource):

    _schema = ClusterNodesExternalCacheSchema
