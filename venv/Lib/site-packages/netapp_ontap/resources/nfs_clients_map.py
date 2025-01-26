r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
ONTAP connected clients map functionality is mainly used by the System Manager to display client information.<p/>
The following are details of the fields retrieved for the Connected Clients MAP GET API:<p/>
node.name: Node name hosting this record, essentially the node hosting the "server_ip".
node.uuid: Node UUID hosting this record, essentially the node hosting the "server_ip".
svm.name: SVM name to which the "server_ip" belongs to.
svm.uuid: SVM UUID to which the "server_ip" belongs to.
server_ip: All clients that are connected to this interface are displayed in rows.
client_ips: List of client IP addresses connected to the interface.
## Example
### Retrieving connected client Map information
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsClientsMap

with HostConnection(
    "<cluster-mgmt-ip>", username="admin", password="password", verify=False
):
    print(list(NfsClientsMap.get_collection(return_timeout=15)))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    NfsClientsMap(
        {
            "server_ip": "10.140.72.214",
            "svm": {"name": "vs1", "uuid": "c642db55-b8d0-11e9-9ad1-0050568e8480"},
            "node": {"name": "vsim1", "uuid": "cc282893-b82f-11e9-a3ad-0050568e8480"},
            "client_ips": ["127.0.0.1"],
        }
    )
]

```
</div>
</div>
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


__all__ = ["NfsClientsMap", "NfsClientsMapSchema"]
__pdoc__ = {
    "NfsClientsMapSchema.resource": False,
    "NfsClientsMapSchema.opts": False,
    "NfsClientsMap.nfs_clients_map_show": False,
    "NfsClientsMap.nfs_clients_map_create": False,
    "NfsClientsMap.nfs_clients_map_modify": False,
    "NfsClientsMap.nfs_clients_map_delete": False,
}


class NfsClientsMapSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsClientsMap object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the nfs_clients_map."""

    client_ips = fields.List(fields.Str, data_key="client_ips")
    r""" Specifies the IP address of the client.


Example: ["127.0.0.1"]"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the nfs_clients_map."""

    server_ip = fields.Str(
        data_key="server_ip",
    )
    r""" Specifies the IP address of the server."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the nfs_clients_map."""

    @property
    def resource(self):
        return NfsClientsMap

    gettable_fields = [
        "links",
        "client_ips",
        "node.links",
        "node.name",
        "node.uuid",
        "server_ip",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """links,client_ips,node.links,node.name,node.uuid,server_ip,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "client_ips",
        "node.name",
        "node.uuid",
        "server_ip",
        "svm.name",
        "svm.uuid",
    ]
    """client_ips,node.name,node.uuid,server_ip,svm.name,svm.uuid,"""

    postable_fields = [
        "client_ips",
        "node.name",
        "node.uuid",
        "server_ip",
        "svm.name",
        "svm.uuid",
    ]
    """client_ips,node.name,node.uuid,server_ip,svm.name,svm.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in NfsClientsMap.get_collection(fields=field)]
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
            raise NetAppRestError("NfsClientsMap modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class NfsClientsMap(Resource):
    """Allows interaction with NfsClientsMap objects on the host"""

    _schema = NfsClientsMapSchema
    _path = "/api/protocols/nfs/connected-client-maps"

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves NFS clients information.

### Learn more
* [`DOC /protocols/nfs/connected-client-maps`](#docs-NAS-protocols_nfs_connected-client-maps)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs clients map show")
        def nfs_clients_map_show(
            fields: List[Choices.define(["client_ips", "server_ip", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of NfsClientsMap resources

            Args:
                client_ips: Specifies the IP address of the client. 
                server_ip: Specifies the IP address of the server. 
            """

            kwargs = {}
            if client_ips is not None:
                kwargs["client_ips"] = client_ips
            if server_ip is not None:
                kwargs["server_ip"] = server_ip
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return NfsClientsMap.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all NfsClientsMap resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves NFS clients information.

### Learn more
* [`DOC /protocols/nfs/connected-client-maps`](#docs-NAS-protocols_nfs_connected-client-maps)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)






