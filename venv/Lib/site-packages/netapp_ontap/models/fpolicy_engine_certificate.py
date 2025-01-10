r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["FpolicyEngineCertificate", "FpolicyEngineCertificateSchema"]
__pdoc__ = {
    "FpolicyEngineCertificateSchema.resource": False,
    "FpolicyEngineCertificateSchema.opts": False,
    "FpolicyEngineCertificate": False,
}


class FpolicyEngineCertificateSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the FpolicyEngineCertificate object"""

    ca = fields.Str(data_key="ca")
    r""" Specifies the certificate authority (CA) name of the certificate
used for authentication if SSL authentication between the SVM and the FPolicy
server is configured.


Example: TASample1 """

    name = fields.Str(data_key="name")
    r""" Specifies the certificate name as a fully qualified domain
name (FQDN) or custom common name. The certificate is used if SSL authentication
between the SVM and the FPolicy server is configured.


Example: Sample1-FPolicy-Client """

    serial_number = fields.Str(data_key="serial_number")
    r""" Specifies the serial number of the certificate used for
authentication if SSL authentication between the SVM and the FPolicy
server is configured.


Example: 8DDE112A114D1FBC """

    @property
    def resource(self):
        return FpolicyEngineCertificate

    gettable_fields = [
        "ca",
        "name",
        "serial_number",
    ]
    """ca,name,serial_number,"""

    patchable_fields = [
        "ca",
        "name",
        "serial_number",
    ]
    """ca,name,serial_number,"""

    postable_fields = [
        "ca",
        "name",
        "serial_number",
    ]
    """ca,name,serial_number,"""


class FpolicyEngineCertificate(Resource):

    _schema = FpolicyEngineCertificateSchema
