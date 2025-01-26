r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["BroadcastDomainReferenceIpspace", "BroadcastDomainReferenceIpspaceSchema"]
__pdoc__ = {
    "BroadcastDomainReferenceIpspaceSchema.resource": False,
    "BroadcastDomainReferenceIpspaceSchema.opts": False,
    "BroadcastDomainReferenceIpspace": False,
}


class BroadcastDomainReferenceIpspaceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the BroadcastDomainReferenceIpspace object"""

    name = fields.Str(data_key="name")
    r""" Name of the broadcast domain's IPspace

Example: ipspace1 """

    @property
    def resource(self):
        return BroadcastDomainReferenceIpspace

    gettable_fields = [
        "name",
    ]
    """name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class BroadcastDomainReferenceIpspace(Resource):

    _schema = BroadcastDomainReferenceIpspaceSchema
