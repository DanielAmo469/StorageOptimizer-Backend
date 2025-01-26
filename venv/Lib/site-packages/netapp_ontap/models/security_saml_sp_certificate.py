r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["SecuritySamlSpCertificate", "SecuritySamlSpCertificateSchema"]
__pdoc__ = {
    "SecuritySamlSpCertificateSchema.resource": False,
    "SecuritySamlSpCertificateSchema.opts": False,
    "SecuritySamlSpCertificate": False,
}


class SecuritySamlSpCertificateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the SecuritySamlSpCertificate object"""

    ca = fields.Str(data_key="ca")
    r""" Server certificate issuing certificate authority (CA).  This cannot be used with the server certificate common name. """

    common_name = fields.Str(data_key="common_name")
    r""" Server certificate common name.  This cannot be used with the certificate authority (CA) or serial_number.

Example: cluster1 """

    serial_number = fields.Str(data_key="serial_number")
    r""" Server certificate serial number.  This cannot be used with the server certificate common name.

Example: 1506B24A94F566BA """

    @property
    def resource(self):
        return SecuritySamlSpCertificate

    gettable_fields = [
        "ca",
        "common_name",
        "serial_number",
    ]
    """ca,common_name,serial_number,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
        "ca",
        "common_name",
        "serial_number",
    ]
    """ca,common_name,serial_number,"""


class SecuritySamlSpCertificate(Resource):

    _schema = SecuritySamlSpCertificateSchema
