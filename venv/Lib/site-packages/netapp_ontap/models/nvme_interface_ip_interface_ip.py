r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["NvmeInterfaceIpInterfaceIp", "NvmeInterfaceIpInterfaceIpSchema"]
__pdoc__ = {
    "NvmeInterfaceIpInterfaceIpSchema.resource": False,
    "NvmeInterfaceIpInterfaceIpSchema.opts": False,
    "NvmeInterfaceIpInterfaceIp": False,
}


class NvmeInterfaceIpInterfaceIpSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NvmeInterfaceIpInterfaceIp object"""

    address = fields.Str(data_key="address")
    r""" The address field of the nvme_interface_ip_interface_ip. """

    @property
    def resource(self):
        return NvmeInterfaceIpInterfaceIp

    gettable_fields = [
        "address",
    ]
    """address,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class NvmeInterfaceIpInterfaceIp(Resource):

    _schema = NvmeInterfaceIpInterfaceIpSchema
