r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventMessage", "EmsEventMessageSchema"]
__pdoc__ = {
    "EmsEventMessageSchema.resource": False,
    "EmsEventMessageSchema.opts": False,
    "EmsEventMessage": False,
}


class EmsEventMessageSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventMessage object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_event_message. """

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
        return EmsEventMessage

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


class EmsEventMessage(Resource):

    _schema = EmsEventMessageSchema
