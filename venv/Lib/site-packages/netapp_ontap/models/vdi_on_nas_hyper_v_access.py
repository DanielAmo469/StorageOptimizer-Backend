r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["VdiOnNasHyperVAccess", "VdiOnNasHyperVAccessSchema"]
__pdoc__ = {
    "VdiOnNasHyperVAccessSchema.resource": False,
    "VdiOnNasHyperVAccessSchema.opts": False,
    "VdiOnNasHyperVAccess": False,
}


class VdiOnNasHyperVAccessSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VdiOnNasHyperVAccess object"""

    service_account = fields.Str(data_key="service_account")
    r""" Hyper-V service account. """

    @property
    def resource(self):
        return VdiOnNasHyperVAccess

    gettable_fields = [
        "service_account",
    ]
    """service_account,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "service_account",
    ]
    """service_account,"""


class VdiOnNasHyperVAccess(Resource):

    _schema = VdiOnNasHyperVAccessSchema
