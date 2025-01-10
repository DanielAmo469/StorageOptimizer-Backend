r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["LicenseManagerResponseRecords", "LicenseManagerResponseRecordsSchema"]
__pdoc__ = {
    "LicenseManagerResponseRecordsSchema.resource": False,
    "LicenseManagerResponseRecordsSchema.opts": False,
    "LicenseManagerResponseRecords": False,
}


class LicenseManagerResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LicenseManagerResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the license_manager_response_records. """

    default = fields.Boolean(data_key="default")
    r""" Flag that indicates whether it's the default license manager instance used by the cluster.'
When a capacity pool is created and if the license manager field is omitted, it is assumed that the license of the capacity pool is installed on the default license manager instance. """

    uri = fields.Nested("netapp_ontap.models.license_manager_uri.LicenseManagerUriSchema", unknown=EXCLUDE, data_key="uri")
    r""" The uri field of the license_manager_response_records. """

    uuid = fields.Str(data_key="uuid")
    r""" The uuid field of the license_manager_response_records.

Example: 4ea7a442-86d1-11e0-ae1c-112233445566 """

    @property
    def resource(self):
        return LicenseManagerResponseRecords

    gettable_fields = [
        "links",
        "default",
        "uri",
        "uuid",
    ]
    """links,default,uri,uuid,"""

    patchable_fields = [
        "uri",
    ]
    """uri,"""

    postable_fields = [
        "uri",
    ]
    """uri,"""


class LicenseManagerResponseRecords(Resource):

    _schema = LicenseManagerResponseRecordsSchema
