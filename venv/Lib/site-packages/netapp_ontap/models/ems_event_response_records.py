r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["EmsEventResponseRecords", "EmsEventResponseRecordsSchema"]
__pdoc__ = {
    "EmsEventResponseRecordsSchema.resource": False,
    "EmsEventResponseRecordsSchema.opts": False,
    "EmsEventResponseRecords": False,
}


class EmsEventResponseRecordsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the EmsEventResponseRecords object"""

    links = fields.Nested("netapp_ontap.models.ems_event_links.EmsEventLinksSchema", unknown=EXCLUDE, data_key="_links")
    r""" The links field of the ems_event_response_records. """

    index = Size(data_key="index")
    r""" Index of the event. Returned by default.

Example: 1 """

    log_message = fields.Str(data_key="log_message")
    r""" A formatted text string populated with parameter details. Returned by default. """

    message = fields.Nested("netapp_ontap.models.ems_event_message1.EmsEventMessage1Schema", unknown=EXCLUDE, data_key="message")
    r""" The message field of the ems_event_response_records. """

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", unknown=EXCLUDE, data_key="node")
    r""" The node field of the ems_event_response_records. """

    parameters = fields.List(fields.Nested("netapp_ontap.models.ems_event_parameters.EmsEventParametersSchema", unknown=EXCLUDE), data_key="parameters")
    r""" A list of parameters provided with the EMS event. """

    source = fields.Str(data_key="source")
    r""" Source """

    time = ImpreciseDateTime(data_key="time")
    r""" Timestamp of the event. Returned by default. """

    @property
    def resource(self):
        return EmsEventResponseRecords

    gettable_fields = [
        "links",
        "index",
        "log_message",
        "message",
        "node.links",
        "node.name",
        "node.uuid",
        "parameters",
        "source",
        "time",
    ]
    """links,index,log_message,message,node.links,node.name,node.uuid,parameters,source,time,"""

    patchable_fields = [
        "links",
        "log_message",
        "message",
        "node.name",
        "node.uuid",
    ]
    """links,log_message,message,node.name,node.uuid,"""

    postable_fields = [
        "links",
        "message",
        "node.name",
        "node.uuid",
    ]
    """links,message,node.name,node.uuid,"""


class EmsEventResponseRecords(Resource):

    _schema = EmsEventResponseRecordsSchema
