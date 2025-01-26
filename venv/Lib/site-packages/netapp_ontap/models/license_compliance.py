r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LicenseCompliance", "LicenseComplianceSchema"]
__pdoc__ = {
    "LicenseComplianceSchema.resource": False,
    "LicenseComplianceSchema.opts": False,
    "LicenseCompliance": False,
}


class LicenseComplianceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LicenseCompliance object"""

    state = fields.Str(data_key="state")
    r""" Compliance state of the license.

Valid choices:

* compliant
* noncompliant
* unlicensed
* unknown """

    @property
    def resource(self):
        return LicenseCompliance

    gettable_fields = [
        "state",
    ]
    """state,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class LicenseCompliance(Resource):

    _schema = LicenseComplianceSchema
