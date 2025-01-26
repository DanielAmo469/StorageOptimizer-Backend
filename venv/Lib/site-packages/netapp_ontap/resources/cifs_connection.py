r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
ONTAP CIFS connections show functionality is used to display currently established CIFS connections.
### Information on the CIFS connection

* Retrieve the list of the established CIFS connections
## Example
### Retrieving established connection information
To retrieve the list of CIFS connections, use the following API.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsConnection

with HostConnection(
    "<cluster-mgmt-ip>", username="admin", password="password", verify=False
):
    print(list(CifsConnection.get_collection(return_timeout=15)))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    CifsConnection(
        {
            "network_context_id": 3,
            "identifier": 91842,
            "client_ip": "10.74.7.182",
            "server_ip": "10.140.70.197",
            "node": {
                "name": "ganeshkr-vsim1",
                "uuid": "85d46998-4e5d-11ea-afb1-0050568ec4e4",
            },
            "svm": {"name": "vs1", "uuid": "fc824aa8-4e60-11ea-afb1-0050568ec4e4"},
            "sessions": [{"identifier": 625718873227788312}],
            "client_port": 12345,
        }
    ),
    CifsConnection(
        {
            "network_context_id": 5,
            "identifier": 92080,
            "client_ip": "10.140.133.97",
            "server_ip": "10.140.70.197",
            "node": {
                "name": "ganeshkr-vsim1",
                "uuid": "85d46998-4e5d-11ea-afb1-0050568ec4e4",
            },
            "svm": {"name": "vs1", "uuid": "fc824aa8-4e60-11ea-afb1-0050568ec4e4"},
            "sessions": [
                {"identifier": 625718873227788579},
                {"identifier": 625718873227788577},
            ],
            "client_port": 23413,
        }
    ),
]

```
</div>
</div>

  ---"""

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


__all__ = ["CifsConnection", "CifsConnectionSchema"]
__pdoc__ = {
    "CifsConnectionSchema.resource": False,
    "CifsConnectionSchema.opts": False,
    "CifsConnection.cifs_connection_show": False,
    "CifsConnection.cifs_connection_create": False,
    "CifsConnection.cifs_connection_modify": False,
    "CifsConnection.cifs_connection_delete": False,
}


class CifsConnectionSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsConnection object"""

    client_ip = fields.Str(
        data_key="client_ip",
    )
    r""" Specifies IP of the client.


Example: 10.74.7.182"""

    client_port = Size(
        data_key="client_port",
    )
    r""" "A unique 32-bit unsigned number used to represent the port number of the connection".


Example: 12345"""

    identifier = Size(
        data_key="identifier",
    )
    r""" A unique 32-bit unsigned number used to represent each SMB session's connection ID.


Example: 22802"""

    network_context_id = Size(
        data_key="network_context_id",
    )
    r""" A unique 32-bit unsigned number used to represent each SMB session's network context ID.


Example: 22802"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the cifs_connection."""

    server_ip = fields.Str(
        data_key="server_ip",
    )
    r""" Specifies the IP address of the SVM.


Example: 10.140.78.248"""

    sessions = fields.List(fields.Nested("netapp_ontap.models.cifs_connection_sessions.CifsConnectionSessionsSchema", unknown=EXCLUDE), data_key="sessions")
    r""" The sessions field of the cifs_connection."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the cifs_connection."""

    @property
    def resource(self):
        return CifsConnection

    gettable_fields = [
        "client_ip",
        "client_port",
        "identifier",
        "network_context_id",
        "node.links",
        "node.name",
        "node.uuid",
        "server_ip",
        "sessions",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """client_ip,client_port,identifier,network_context_id,node.links,node.name,node.uuid,server_ip,sessions,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "node.name",
        "node.uuid",
        "sessions",
        "svm.name",
        "svm.uuid",
    ]
    """node.name,node.uuid,sessions,svm.name,svm.uuid,"""

    postable_fields = [
        "node.name",
        "node.uuid",
        "sessions",
        "svm.name",
        "svm.uuid",
    ]
    """node.name,node.uuid,sessions,svm.name,svm.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in CifsConnection.get_collection(fields=field)]
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
            raise NetAppRestError("CifsConnection modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class CifsConnection(Resource):
    """Allows interaction with CifsConnection objects on the host"""

    _schema = CifsConnectionSchema
    _path = "/api/protocols/cifs/connections"

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the CIFS connection information for all SVMs.
### Related ONTAP commands
  * `vserver cifs connection show`
### Learn more
* [`DOC /protocols/cifs/connections`](#docs-NAS-protocols_cifs_connections)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="cifs connection show")
        def cifs_connection_show(
            fields: List[Choices.define(["client_ip", "client_port", "identifier", "network_context_id", "server_ip", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of CifsConnection resources

            Args:
                client_ip: Specifies IP of the client. 
                client_port: \"A unique 32-bit unsigned number used to represent the port number of the connection\". 
                identifier: A unique 32-bit unsigned number used to represent each SMB session's connection ID. 
                network_context_id: A unique 32-bit unsigned number used to represent each SMB session's network context ID. 
                server_ip: Specifies the IP address of the SVM. 
            """

            kwargs = {}
            if client_ip is not None:
                kwargs["client_ip"] = client_ip
            if client_port is not None:
                kwargs["client_port"] = client_port
            if identifier is not None:
                kwargs["identifier"] = identifier
            if network_context_id is not None:
                kwargs["network_context_id"] = network_context_id
            if server_ip is not None:
                kwargs["server_ip"] = server_ip
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return CifsConnection.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all CifsConnection resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the CIFS connection information for all SVMs.
### Related ONTAP commands
  * `vserver cifs connection show`
### Learn more
* [`DOC /protocols/cifs/connections`](#docs-NAS-protocols_cifs_connections)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)






