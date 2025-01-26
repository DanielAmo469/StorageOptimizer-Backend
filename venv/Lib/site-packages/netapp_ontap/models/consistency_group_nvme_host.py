r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ConsistencyGroupNvmeHost", "ConsistencyGroupNvmeHostSchema"]
__pdoc__ = {
    "ConsistencyGroupNvmeHostSchema.resource": False,
    "ConsistencyGroupNvmeHostSchema.opts": False,
    "ConsistencyGroupNvmeHost": False,
}


class ConsistencyGroupNvmeHostSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ConsistencyGroupNvmeHost object"""

    nqn = fields.Str(data_key="nqn")
    r""" The NVMe qualified name (NQN) used to identify the NVMe storage target. Not allowed in POST when the `records` property is used.


Example: nqn.1992-01.example.com:string """

    @property
    def resource(self):
        return ConsistencyGroupNvmeHost

    gettable_fields = [
        "nqn",
    ]
    """nqn,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "nqn",
    ]
    """nqn,"""


class ConsistencyGroupNvmeHost(Resource):

    _schema = ConsistencyGroupNvmeHostSchema
