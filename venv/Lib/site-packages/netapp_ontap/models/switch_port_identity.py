r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SwitchPortIdentity", "SwitchPortIdentitySchema"]
__pdoc__ = {
    "SwitchPortIdentitySchema.resource": False,
    "SwitchPortIdentitySchema.opts": False,
    "SwitchPortIdentity": False,
}


class SwitchPortIdentitySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SwitchPortIdentity object"""

    index = Size(data_key="index")
    r""" Interface Index. """

    name = fields.Str(data_key="name")
    r""" Interface Name. """

    number = Size(data_key="number")
    r""" Interface Number. """

    @property
    def resource(self):
        return SwitchPortIdentity

    gettable_fields = [
        "index",
        "name",
        "number",
    ]
    """index,name,number,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class SwitchPortIdentity(Resource):

    _schema = SwitchPortIdentitySchema
