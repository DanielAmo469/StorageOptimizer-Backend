r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SecurityCertificateRefUuid", "SecurityCertificateRefUuidSchema"]
__pdoc__ = {
    "SecurityCertificateRefUuidSchema.resource": False,
    "SecurityCertificateRefUuidSchema.opts": False,
    "SecurityCertificateRefUuid": False,
}


class SecurityCertificateRefUuidSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SecurityCertificateRefUuid object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the security_certificate_ref_uuid. """

    uuid = fields.Str(data_key="uuid")
    r""" Certificate UUID

Example: 1cd8a442-86d1-11e0-ae1c-123478563412 """

    @property
    def resource(self):
        return SecurityCertificateRefUuid

    gettable_fields = [
        "links",
        "uuid",
    ]
    """links,uuid,"""

    patchable_fields = [
        "uuid",
    ]
    """uuid,"""

    postable_fields = [
        "uuid",
    ]
    """uuid,"""


class SecurityCertificateRefUuid(Resource):

    _schema = SecurityCertificateRefUuidSchema
