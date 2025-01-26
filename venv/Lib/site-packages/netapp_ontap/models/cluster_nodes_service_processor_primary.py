r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesServiceProcessorPrimary", "ClusterNodesServiceProcessorPrimarySchema"]
__pdoc__ = {
    "ClusterNodesServiceProcessorPrimarySchema.resource": False,
    "ClusterNodesServiceProcessorPrimarySchema.opts": False,
    "ClusterNodesServiceProcessorPrimary": False,
}


class ClusterNodesServiceProcessorPrimarySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesServiceProcessorPrimary object"""

    is_current = fields.Boolean(data_key="is_current")
    r""" Indicates whether the service processor is currently booted from the primary partition. """

    state = fields.Str(data_key="state")
    r""" Status of the primary partition.

Valid choices:

* installed
* corrupt
* updating
* auto_updating
* none """

    version = fields.Str(data_key="version")
    r""" Firmware version of the primary partition.

Example: 11.6 """

    @property
    def resource(self):
        return ClusterNodesServiceProcessorPrimary

    gettable_fields = [
        "is_current",
        "state",
        "version",
    ]
    """is_current,state,version,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class ClusterNodesServiceProcessorPrimary(Resource):

    _schema = ClusterNodesServiceProcessorPrimarySchema
