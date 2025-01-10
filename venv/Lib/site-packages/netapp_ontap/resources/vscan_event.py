r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

"""

import asyncio
from datetime import datetime
import inspect
from typing import Callable, Iterable, List, Optional, Union

try:
    RECLINE_INSTALLED = False
    import recline
    from recline.arg_types.choices import Choices
    from recline.commands import ReclineCommandError
    from netapp_ontap.resource_table import ResourceTable
    RECLINE_INSTALLED = True
except ImportError:
    pass

from marshmallow import fields, EXCLUDE  # type: ignore

import netapp_ontap
from netapp_ontap.resource import Resource, ResourceSchema, ResourceSchemaMeta, ImpreciseDateTime, Size
from netapp_ontap import NetAppResponse, HostConnection
from netapp_ontap.validations import enum_validation, len_validation, integer_validation
from netapp_ontap.error import NetAppRestError


__all__ = ["VscanEvent", "VscanEventSchema"]
__pdoc__ = {
    "VscanEventSchema.resource": False,
    "VscanEventSchema.opts": False,
    "VscanEvent.vscan_event_show": False,
    "VscanEvent.vscan_event_create": False,
    "VscanEvent.vscan_event_modify": False,
    "VscanEvent.vscan_event_delete": False,
}


class VscanEventSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the VscanEvent object"""

    disconnect_reason = fields.Str(
        data_key="disconnect_reason",
        validate=enum_validation(['na', 'vscan_disabled', 'no_data_lif', 'session_uninitialized', 'remote_closed', 'invalid_protocol_msg', 'invalid_session_id', 'inactive_connection', 'invalid_user', 'server_removed']),
    )
    r""" Specifies the reason of the Vscan server disconnection.
The available values are:

* na                        Not applicable
* vscan_disabled            Vscan disabled on the SVM
* no_data_lif               SVM does not have data lif on the node
* session_uninitialized     Session not initialized
* remote_closed             Closure from Server
* invalid_protocol_msg      Invalid protocol-message received
* invalid_session_id        Invalid session-id received
* inactive_connection       No activity on connection
* invalid_user              Connection request by invalid user
* server_removed            Server removed from the active scanner-pool


Valid choices:

* na
* vscan_disabled
* no_data_lif
* session_uninitialized
* remote_closed
* invalid_protocol_msg
* invalid_session_id
* inactive_connection
* invalid_user
* server_removed"""

    event_time = ImpreciseDateTime(
        data_key="event_time",
    )
    r""" Specifies the Timestamp of the event.

Example: 2021-11-25T04:29:41.606+0000"""

    file_path = fields.Str(
        data_key="file_path",
    )
    r""" Specifies the file for which event happened.

Example: /1"""

    interface = fields.Nested("netapp_ontap.resources.ip_interface.IpInterfaceSchema", data_key="interface", unknown=EXCLUDE)
    r""" The interface field of the vscan_event."""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the vscan_event."""

    server = fields.Str(
        data_key="server",
    )
    r""" Specifies the IP address of the Vscan server.

Example: 192.168.1.1"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the vscan_event."""

    type = fields.Str(
        data_key="type",
        validate=enum_validation(['scanner_connected', 'scanner_disconnected', 'scanner_updated', 'scan_internal_error', 'scan_failed', 'scan_timedout', 'file_infected', 'file_renamed', 'file_quarantined', 'file_deleted', 'scanner_busy']),
    )
    r""" Specifies the event type.

Valid choices:

* scanner_connected
* scanner_disconnected
* scanner_updated
* scan_internal_error
* scan_failed
* scan_timedout
* file_infected
* file_renamed
* file_quarantined
* file_deleted
* scanner_busy"""

    vendor = fields.Str(
        data_key="vendor",
    )
    r""" Specifies the scan-engine vendor.

Example: mighty master anti-evil scanner"""

    version = fields.Str(
        data_key="version",
    )
    r""" Specifies the scan-engine version.

Example: 1.0"""

    @property
    def resource(self):
        return VscanEvent

    gettable_fields = [
        "disconnect_reason",
        "event_time",
        "file_path",
        "interface.links",
        "interface.ip",
        "interface.name",
        "interface.uuid",
        "node.links",
        "node.name",
        "node.uuid",
        "server",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "type",
        "vendor",
        "version",
    ]
    """disconnect_reason,event_time,file_path,interface.links,interface.ip,interface.name,interface.uuid,node.links,node.name,node.uuid,server,svm.links,svm.name,svm.uuid,type,vendor,version,"""

    patchable_fields = [
        "event_time",
        "file_path",
        "interface.ip",
        "interface.name",
        "interface.uuid",
        "node.name",
        "node.uuid",
        "server",
        "svm.name",
        "svm.uuid",
        "type",
        "vendor",
        "version",
    ]
    """event_time,file_path,interface.ip,interface.name,interface.uuid,node.name,node.uuid,server,svm.name,svm.uuid,type,vendor,version,"""

    postable_fields = [
        "event_time",
        "file_path",
        "interface.ip",
        "interface.name",
        "interface.uuid",
        "node.name",
        "node.uuid",
        "server",
        "svm.name",
        "svm.uuid",
        "type",
        "vendor",
        "version",
    ]
    """event_time,file_path,interface.ip,interface.name,interface.uuid,node.name,node.uuid,server,svm.name,svm.uuid,type,vendor,version,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in VscanEvent.get_collection(fields=field)]
    return getter

async def _wait_for_job(response: NetAppResponse) -> None:
    """Examine the given response. If it is a job, asynchronously wait for it to
    complete. While polling, prints the current status message of the job.
    """

    if not response.is_job:
        return
    from netapp_ontap.resources import Job
    job = Job(**response.http_response.json()["job"])
    while True:
        job.get(fields="state,message")
        if hasattr(job, "message"):
            print("[%s]: %s" % (job.state, job.message))
        if job.state == "failure":
            raise NetAppRestError("VscanEvent modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class VscanEvent(Resource):
    """Allows interaction with VscanEvent objects on the host"""

    _schema = VscanEventSchema
    _path = "/api/protocols/vscan/{svm[uuid]}/events"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves Vscan events, which are generated by the cluster to capture important events.
### Related ONTAP commands
* `vserver vscan show-events`
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="vscan event show")
        def vscan_event_show(
            svm_uuid,
            disconnect_reason: Choices.define(_get_field_list("disconnect_reason"), cache_choices=True, inexact=True)=None,
            event_time: Choices.define(_get_field_list("event_time"), cache_choices=True, inexact=True)=None,
            file_path: Choices.define(_get_field_list("file_path"), cache_choices=True, inexact=True)=None,
            server: Choices.define(_get_field_list("server"), cache_choices=True, inexact=True)=None,
            type: Choices.define(_get_field_list("type"), cache_choices=True, inexact=True)=None,
            vendor: Choices.define(_get_field_list("vendor"), cache_choices=True, inexact=True)=None,
            version: Choices.define(_get_field_list("version"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["disconnect_reason", "event_time", "file_path", "server", "type", "vendor", "version", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of VscanEvent resources

            Args:
                disconnect_reason: Specifies the reason of the Vscan server disconnection. The available values are: * na                        Not applicable * vscan_disabled            Vscan disabled on the SVM * no_data_lif               SVM does not have data lif on the node * session_uninitialized     Session not initialized * remote_closed             Closure from Server * invalid_protocol_msg      Invalid protocol-message received * invalid_session_id        Invalid session-id received * inactive_connection       No activity on connection * invalid_user              Connection request by invalid user * server_removed            Server removed from the active scanner-pool 
                event_time: Specifies the Timestamp of the event.
                file_path: Specifies the file for which event happened.
                server: Specifies the IP address of the Vscan server.
                type: Specifies the event type.
                vendor: Specifies the scan-engine vendor.
                version: Specifies the scan-engine version.
            """

            kwargs = {}
            if disconnect_reason is not None:
                kwargs["disconnect_reason"] = disconnect_reason
            if event_time is not None:
                kwargs["event_time"] = event_time
            if file_path is not None:
                kwargs["file_path"] = file_path
            if server is not None:
                kwargs["server"] = server
            if type is not None:
                kwargs["type"] = type
            if vendor is not None:
                kwargs["vendor"] = vendor
            if version is not None:
                kwargs["version"] = version
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return VscanEvent.get_collection(
                svm_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all VscanEvent resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves Vscan events, which are generated by the cluster to capture important events.
### Related ONTAP commands
* `vserver vscan show-events`
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)






