r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsDestinationResponseRecords", "EmsDestinationResponseRecordsSchema"]
__pdoc__ = {
    "EmsDestinationResponseRecordsSchema.resource": False,
    "EmsDestinationResponseRecordsSchema.opts": False,
    "EmsDestinationResponseRecords": False,
}


class EmsDestinationResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsDestinationResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_destination_response_records. """

    access_control_role = fields.Nested("netapp_ontap.resources.role.RoleSchema", unknown=EXCLUDE, data_key="access_control_role")
    r""" The access_control_role field of the ems_destination_response_records. """

    certificate = fields.Nested("netapp_ontap.models.ems_certificate.EmsCertificateSchema", unknown=EXCLUDE, data_key="certificate")
    r""" The certificate field of the ems_destination_response_records. """

    connectivity = fields.Nested("netapp_ontap.models.ems_destination_response_records_connectivity.EmsDestinationResponseRecordsConnectivitySchema", unknown=EXCLUDE, data_key="connectivity")
    r""" The connectivity field of the ems_destination_response_records. """

    destination = fields.Str(data_key="destination")
    r""" Event destination

Example: administrator@mycompany.com """

    filters = fields.List(fields.Nested("netapp_ontap.models.ems_destination_filters.EmsDestinationFiltersSchema", unknown=EXCLUDE), data_key="filters")
    r""" The filters field of the ems_destination_response_records. """

    name = fields.Str(data_key="name")
    r""" Destination name.  Valid in POST.

Example: Admin_Email """

    syslog = fields.Nested("netapp_ontap.models.ems_syslog.EmsSyslogSchema", unknown=EXCLUDE, data_key="syslog")
    r""" The syslog field of the ems_destination_response_records. """

    system_defined = fields.Boolean(data_key="system_defined")
    r""" Flag indicating system-defined destinations.

Example: true """

    type = fields.Str(data_key="type")
    r""" Type of destination. Valid in POST.

Valid choices:

* snmp
* email
* syslog
* rest_api """

    @property
    def resource(self):
        return EmsDestinationResponseRecords

    gettable_fields = [
        "links",
        "access_control_role.links",
        "access_control_role.name",
        "certificate",
        "connectivity",
        "destination",
        "filters.links",
        "filters.name",
        "name",
        "syslog",
        "system_defined",
        "type",
    ]
    """links,access_control_role.links,access_control_role.name,certificate,connectivity,destination,filters.links,filters.name,name,syslog,system_defined,type,"""

    patchable_fields = [
        "certificate",
        "connectivity",
        "destination",
        "filters.name",
        "syslog",
    ]
    """certificate,connectivity,destination,filters.name,syslog,"""

    postable_fields = [
        "certificate",
        "connectivity",
        "destination",
        "filters.name",
        "name",
        "syslog",
        "type",
    ]
    """certificate,connectivity,destination,filters.name,name,syslog,type,"""


class EmsDestinationResponseRecords(Resource):

    _schema = EmsDestinationResponseRecordsSchema
