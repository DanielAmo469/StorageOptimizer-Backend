r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["IgroupTarget", "IgroupTargetSchema"]
__pdoc__ = {
    "IgroupTargetSchema.resource": False,
    "IgroupTargetSchema.opts": False,
    "IgroupTarget": False,
}


class IgroupTargetSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the IgroupTarget object"""

    firmware_revision = fields.Str(data_key="firmware_revision")
    r""" The firmware revision of the SCSI target specific to the OS type of the initiator group.


Example: 9111 """

    product_id = fields.Str(data_key="product_id")
    r""" The product ID of the SCSI target.


Example: LUN C-Mode """

    vendor_id = fields.Str(data_key="vendor_id")
    r""" The vendor ID of the SCSI target.


Example: NETAPP """

    @property
    def resource(self):
        return IgroupTarget

    gettable_fields = [
        "firmware_revision",
        "product_id",
        "vendor_id",
    ]
    """firmware_revision,product_id,vendor_id,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class IgroupTarget(Resource):

    _schema = IgroupTargetSchema
