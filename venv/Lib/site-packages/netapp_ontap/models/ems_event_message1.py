r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventMessage1", "EmsEventMessage1Schema"]
__pdoc__ = {
    "EmsEventMessage1Schema.resource": False,
    "EmsEventMessage1Schema.opts": False,
    "EmsEventMessage1": False,
}


class EmsEventMessage1Schema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventMessage1 object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_event_message1. """

    name = fields.Str(data_key="name")
    r""" Message name of the event. Returned by default.

Example: callhome.spares.low """

    severity = fields.Str(data_key="severity")
    r""" Severity of the event. Returned by default.

Valid choices:

* emergency
* alert
* error
* notice
* informational
* debug """

    @property
    def resource(self):
        return EmsEventMessage1

    gettable_fields = [
        "links",
        "name",
        "severity",
    ]
    """links,name,severity,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""


class EmsEventMessage1(Resource):

    _schema = EmsEventMessage1Schema
