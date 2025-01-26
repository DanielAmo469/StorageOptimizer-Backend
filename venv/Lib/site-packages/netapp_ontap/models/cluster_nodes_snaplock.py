r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ClusterNodesSnaplock", "ClusterNodesSnaplockSchema"]
__pdoc__ = {
    "ClusterNodesSnaplockSchema.resource": False,
    "ClusterNodesSnaplockSchema.opts": False,
    "ClusterNodesSnaplock": False,
}


class ClusterNodesSnaplockSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ClusterNodesSnaplock object"""

    compliance_clock_time = ImpreciseDateTime(data_key="compliance_clock_time")
    r""" SnapLock compliance clock time.

Example: 2018-06-04T19:00:00.000+0000 """

    @property
    def resource(self):
        return ClusterNodesSnaplock

    gettable_fields = [
        "compliance_clock_time",
    ]
    """compliance_clock_time,"""

    patchable_fields = [
        "compliance_clock_time",
    ]
    """compliance_clock_time,"""

    postable_fields = [
        "compliance_clock_time",
    ]
    """compliance_clock_time,"""


class ClusterNodesSnaplock(Resource):

    _schema = ClusterNodesSnaplockSchema
