r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["ApplicationLunMappingObject", "ApplicationLunMappingObjectSchema"]
__pdoc__ = {
    "ApplicationLunMappingObjectSchema.resource": False,
    "ApplicationLunMappingObjectSchema.opts": False,
    "ApplicationLunMappingObject": False,
}


class ApplicationLunMappingObjectSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ApplicationLunMappingObject object"""

    fcp = fields.List(fields.Nested("netapp_ontap.models.application_san_access_fcp_endpoint.ApplicationSanAccessFcpEndpointSchema", unknown=EXCLUDE), data_key="fcp")
    r""" All possible Fibre Channel Protocol (FCP) access endpoints for the LUN. """

    igroup = fields.Nested("netapp_ontap.models.application_lun_mapping_object_igroup.ApplicationLunMappingObjectIgroupSchema", unknown=EXCLUDE, data_key="igroup")
    r""" The igroup field of the application_lun_mapping_object. """

    iscsi = fields.List(fields.Nested("netapp_ontap.models.application_san_access_iscsi_endpoint.ApplicationSanAccessIscsiEndpointSchema", unknown=EXCLUDE), data_key="iscsi")
    r""" All possible iSCSI access endpoints for the LUN. """

    lun_id = Size(data_key="lun_id")
    r""" LUN ID """

    @property
    def resource(self):
        return ApplicationLunMappingObject

    gettable_fields = [
        "fcp",
        "igroup",
        "iscsi",
        "lun_id",
    ]
    """fcp,igroup,iscsi,lun_id,"""

    patchable_fields = [
        "igroup",
    ]
    """igroup,"""

    postable_fields = [
        "igroup",
    ]
    """igroup,"""


class ApplicationLunMappingObject(Resource):

    _schema = ApplicationLunMappingObjectSchema
