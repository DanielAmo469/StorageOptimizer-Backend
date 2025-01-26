r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeSubsystemControllerAdminQueue", "NvmeSubsystemControllerAdminQueueSchema"]
__pdoc__ = {
    "NvmeSubsystemControllerAdminQueueSchema.resource": False,
    "NvmeSubsystemControllerAdminQueueSchema.opts": False,
    "NvmeSubsystemControllerAdminQueue": False,
}


class NvmeSubsystemControllerAdminQueueSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeSubsystemControllerAdminQueue object"""

    depth = Size(data_key="depth")
    r""" The depth of the admin queue for the controller. """

    @property
    def resource(self):
        return NvmeSubsystemControllerAdminQueue

    gettable_fields = [
        "depth",
    ]
    """depth,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class NvmeSubsystemControllerAdminQueue(Resource):

    _schema = NvmeSubsystemControllerAdminQueueSchema
