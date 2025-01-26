r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["BgpPeerGroupPeer", "BgpPeerGroupPeerSchema"]
__pdoc__ = {
    "BgpPeerGroupPeerSchema.resource": False,
    "BgpPeerGroupPeerSchema.opts": False,
    "BgpPeerGroupPeer": False,
}


class BgpPeerGroupPeerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the BgpPeerGroupPeer object"""

    address = fields.Str(data_key="address")
    r""" Peer router address

Example: 10.10.10.7 """

    asn = Size(data_key="asn")
    r""" Autonomous system number of peer """

    is_next_hop = fields.Boolean(data_key="is_next_hop")
    r""" Use peer address as next hop. """

    @property
    def resource(self):
        return BgpPeerGroupPeer

    gettable_fields = [
        "address",
        "asn",
        "is_next_hop",
    ]
    """address,asn,is_next_hop,"""

    patchable_fields = [
        "address",
        "is_next_hop",
    ]
    """address,is_next_hop,"""

    postable_fields = [
        "address",
        "asn",
        "is_next_hop",
    ]
    """address,asn,is_next_hop,"""


class BgpPeerGroupPeer(Resource):

    _schema = BgpPeerGroupPeerSchema
