r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LunVvolBindingsPartner", "LunVvolBindingsPartnerSchema"]
__pdoc__ = {
    "LunVvolBindingsPartnerSchema.resource": False,
    "LunVvolBindingsPartnerSchema.opts": False,
    "LunVvolBindingsPartner": False,
}


class LunVvolBindingsPartnerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LunVvolBindingsPartner object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the lun_vvol_bindings_partner. """

    name = fields.Str(data_key="name")
    r""" The name of the partner LUN.


Example: /vol/vol1/lun1 """

    uuid = fields.Str(data_key="uuid")
    r""" The unique identifier of the partner LUN.


Example: 4ea7a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return LunVvolBindingsPartner

    gettable_fields = [
        "links",
        "name",
        "uuid",
    ]
    """links,name,uuid,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class LunVvolBindingsPartner(Resource):

    _schema = LunVvolBindingsPartnerSchema
