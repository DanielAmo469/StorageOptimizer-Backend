r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["CifsDomainNameMapping", "CifsDomainNameMappingSchema"]
__pdoc__ = {
    "CifsDomainNameMappingSchema.resource": False,
    "CifsDomainNameMappingSchema.opts": False,
    "CifsDomainNameMapping": False,
}


class CifsDomainNameMappingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsDomainNameMapping object"""

    trusted_domains = fields.List(fields.Str, data_key="trusted_domains")
    r""" The trusted_domains field of the cifs_domain_name_mapping. """

    @property
    def resource(self):
        return CifsDomainNameMapping

    gettable_fields = [
        "trusted_domains",
    ]
    """trusted_domains,"""

    patchable_fields = [
        "trusted_domains",
    ]
    """trusted_domains,"""

    postable_fields = [
        "trusted_domains",
    ]
    """trusted_domains,"""


class CifsDomainNameMapping(Resource):

    _schema = CifsDomainNameMappingSchema
