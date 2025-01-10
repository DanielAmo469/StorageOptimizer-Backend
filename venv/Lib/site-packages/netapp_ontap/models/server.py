r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

from marshmallow import EXCLUDE, fields  # type: ignore
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size


__all__ = ["Server", "ServerSchema"]
__pdoc__ = {
    "ServerSchema.resource": False,
    "ServerSchema.opts": False,
    "Server": False,
}


class ServerSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Server object"""

    ip = fields.Str(data_key="ip")
    r""" Windows Internet Name Server (WINS) address which manages and maps the NetBIOS name of the CIFS server to their network IP addresses. The IP addresses are IPv4 addresses.

Example: 10.224.65.20 """

    state = fields.Str(data_key="state")
    r""" Specifies the state of the WINS server.

Valid choices:

* active
* inactive """

    @property
    def resource(self):
        return Server

    gettable_fields = [
        "ip",
        "state",
    ]
    """ip,state,"""

    patchable_fields = [
        "ip",
        "state",
    ]
    """ip,state,"""

    postable_fields = [
        "ip",
        "state",
    ]
    """ip,state,"""


class Server(Resource):

    _schema = ServerSchema
