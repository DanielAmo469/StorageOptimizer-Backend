r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ZappNvmeRpoLocal", "ZappNvmeRpoLocalSchema"]
__pdoc__ = {
    "ZappNvmeRpoLocalSchema.resource": False,
    "ZappNvmeRpoLocalSchema.opts": False,
    "ZappNvmeRpoLocal": False,
}


class ZappNvmeRpoLocalSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ZappNvmeRpoLocal object"""

    name = fields.Str(data_key="name")
    r""" The local RPO of the application.

Valid choices:

* hourly
* none """

    policy = fields.Str(data_key="policy")
    r""" The Snapshot copy policy to apply to each volume in the smart container. This property is only supported for smart containers. Usage: &lt;snapshot policy&gt; """

    @property
    def resource(self):
        return ZappNvmeRpoLocal

    gettable_fields = [
        "name",
        "policy",
    ]
    """name,policy,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
        "policy",
    ]
    """name,policy,"""


class ZappNvmeRpoLocal(Resource):

    _schema = ZappNvmeRpoLocalSchema
