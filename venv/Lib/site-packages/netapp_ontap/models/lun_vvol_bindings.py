r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunVvolBindings", "LunVvolBindingsSchema"]
__pdoc__ = {
    "LunVvolBindingsSchema.resource": False,
    "LunVvolBindingsSchema.opts": False,
    "LunVvolBindings": False,
}


class LunVvolBindingsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunVvolBindings object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the lun_vvol_bindings. """

    id = Size(data_key="id")
    r""" The ONTAP internal identifier assigned to the vVol binding. The bind identifier is unique amongst all class `vvol` LUNs bound to the same class `protocol_endpoint` LUN.</br>
This property was included in early releases of the REST API for vVols and is maintained for backward compatability. See the `secondary_id` property, which replaces `id`.


Example: 1 """

    partner = fields.Nested("netapp_ontap.models.lun_vvol_bindings_partner.LunVvolBindingsPartnerSchema", unknown=EXCLUDE, data_key="partner")
    r""" The partner field of the lun_vvol_bindings. """

    secondary_id = fields.Str(data_key="secondary_id")
    r""" The identifier assigned to the vVol binding, known as the secondary LUN ID. The identifier is unique amongst all class `vvol` LUNs bound to the same class `protocol_endpoint` LUN.</br>
The format for a secondary LUN ID is 16 hexadecimal digits (zero-filled) followed by a lower case "h".


Example: 0000D20000010000h """

    @property
    def resource(self):
        return LunVvolBindings

    gettable_fields = [
        "links",
        "id",
        "partner",
        "secondary_id",
    ]
    """links,id,partner,secondary_id,"""

    patchable_fields = [
        "partner",
    ]
    """partner,"""

    postable_fields = [
        "partner",
    ]
    """partner,"""


class LunVvolBindings(Resource):

    _schema = LunVvolBindingsSchema
