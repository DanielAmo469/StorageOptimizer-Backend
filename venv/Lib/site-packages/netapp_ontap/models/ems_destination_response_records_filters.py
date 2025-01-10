r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsDestinationResponseRecordsFilters", "EmsDestinationResponseRecordsFiltersSchema"]
__pdoc__ = {
    "EmsDestinationResponseRecordsFiltersSchema.resource": False,
    "EmsDestinationResponseRecordsFiltersSchema.opts": False,
    "EmsDestinationResponseRecordsFilters": False,
}


class EmsDestinationResponseRecordsFiltersSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsDestinationResponseRecordsFilters object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_destination_response_records_filters. """

    name = fields.Str(data_key="name")
    r""" The name field of the ems_destination_response_records_filters.

Example: important-events """

    @property
    def resource(self):
        return EmsDestinationResponseRecordsFilters

    gettable_fields = [
        "links",
        "name",
    ]
    """links,name,"""

    patchable_fields = [
        "name",
    ]
    """name,"""

    postable_fields = [
        "name",
    ]
    """name,"""


class EmsDestinationResponseRecordsFilters(Resource):

    _schema = EmsDestinationResponseRecordsFiltersSchema
