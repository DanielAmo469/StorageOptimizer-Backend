r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

Displays information about NBT connections for the cluster. Displays the IP address associated with the interfaces, the IP addresses of the WINS servers in use, and information about the registered NetBIOS names for the cluster. You can use this command to troubleshoot NetBIOS name resolution problems.
## Examples
### Retrieving full CIFS NetBIOS information for all SVMs in the clsuter
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Netbios

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(Netbios.get_collection(return_timeout=15, fields="*")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    Netbios(
        {
            "scope": "group",
            "time_left": 0,
            "mode": "h",
            "svm": {"name": "vs1", "uuid": "99a3bd71-777f-11ec-95a1-1315568ef5fd"},
            "node": {"name": "vsim2", "uuid": "c2179c2c-777f-11ec-95a1-1315568ef5fd"},
            "suffix": "00",
            "wins_servers": [{"state": "inactive", "ip": "10.10.10.10"}],
            "name": "CIFSERVER2",
            "state": "broadcast",
            "interfaces": ["172.10.144.44"],
            "name_registration_type": "",
        }
    ),
    Netbios(
        {
            "scope": "",
            "time_left": 0,
            "mode": "h",
            "svm": {"name": "vs1", "uuid": "99a3bd71-777f-11ec-95a1-1315568ef5fd"},
            "node": {"name": "vsim2", "uuid": "c2179c2c-777f-11ec-95a1-1315568ef5fd"},
            "suffix": "20",
            "wins_servers": [{"state": "inactive", "ip": "10.10.10.10"}],
            "name": "CIFSERVER2",
            "state": "broadcast",
            "interfaces": ["172.10.144.44"],
            "name_registration_type": "group",
        }
    ),
]

```
</div>
</div>

---
### Retrieving CIFS NetBIOS Information for a particular SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Netbios

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(
        list(
            Netbios.get_collection(
                return_timeout=15,
                fields="*",
                **{"svm.uuid": "45a3bd71-777f-11ec-95a1-1315568ef5fd"}
            )
        )
    )

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
[
    Netbios(
        {
            "scope": "group",
            "time_left": 0,
            "mode": "h",
            "svm": {"name": "vs1", "uuid": "45a3bd71-777f-11ec-95a1-1315568ef5fd"},
            "node": {"name": "vsim2", "uuid": "c2179c2c-777f-11ec-95a1-1315568ef5fd"},
            "suffix": "00",
            "wins_servers": [{"state": "inactive", "ip": "10.10.10.10"}],
            "name": "CIFSERVER2",
            "state": "broadcast",
            "interfaces": ["172.10.144.44"],
            "name_registration_type": "",
        }
    )
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


__all__ = ["Netbios", "NetbiosSchema"]
__pdoc__ = {
    "NetbiosSchema.resource": False,
    "NetbiosSchema.opts": False,
    "Netbios.netbios_show": False,
    "Netbios.netbios_create": False,
    "Netbios.netbios_modify": False,
    "Netbios.netbios_delete": False,
}


class NetbiosSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Netbios object"""

    interfaces = fields.List(fields.Str, data_key="interfaces")
    r""" The interfaces field of the netbios."""

    mode = fields.Str(
        data_key="mode",
        validate=enum_validation(['p', 'h', 'm', 'b']),
    )
    r""" Specifies the mode in which the NetBIOS name service is configured. The supported values are:

  * p - Point to Point
  * h - Hybrid
  * m - Mixed
  * b - Broadcast


Valid choices:

* p
* h
* m
* b"""

    name = fields.Str(
        data_key="name",
    )
    r""" Specifies the NetBIOS name.

Example: CLUSTER_1"""

    name_registration_type = fields.Str(
        data_key="name_registration_type",
        validate=enum_validation(['registered', 'active', 'permanent', 'group', '']),
    )
    r""" Specifies the name registration type.

Valid choices:

* registered
* active
* permanent
* group
*"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the netbios."""

    scope = fields.Str(
        data_key="scope",
    )
    r""" Specifies the NetBIOS name scope. Scope is used as a name for the set of NetBIOS nodes that participate in a NetBIOS over TCP (NBT) virtual LAN."""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['must_register', 'must_unregister', 'wins', 'broadcast', 'name_released', 'wins_conflict', 'broadcast_conflict']),
    )
    r""" Specifies the registration state of the NetBIOS Name.

Valid choices:

* must_register
* must_unregister
* wins
* broadcast
* name_released
* wins_conflict
* broadcast_conflict"""

    suffix = fields.Str(
        data_key="suffix",
    )
    r""" Specifies the NetBIOS suffix. A NetBIOS suffix is the 16th character of the 16-character NetBIOS name. The NetBIOS suffix is used by Microsoft Networking software to identify functionality installed on the registered device."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the netbios."""

    time_left = Size(
        data_key="time_left",
    )
    r""" Specifies the registration time left with WINS, in minutes."""

    wins_servers = fields.List(fields.Nested("netapp_ontap.models.server.ServerSchema", unknown=EXCLUDE), data_key="wins_servers")
    r""" List of WINS"""

    @property
    def resource(self):
        return Netbios

    gettable_fields = [
        "interfaces",
        "mode",
        "name",
        "name_registration_type",
        "node.links",
        "node.name",
        "node.uuid",
        "scope",
        "state",
        "suffix",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "time_left",
        "wins_servers",
    ]
    """interfaces,mode,name,name_registration_type,node.links,node.name,node.uuid,scope,state,suffix,svm.links,svm.name,svm.uuid,time_left,wins_servers,"""

    patchable_fields = [
        "interfaces",
        "mode",
        "name",
        "name_registration_type",
        "node.name",
        "node.uuid",
        "scope",
        "state",
        "suffix",
        "svm.name",
        "svm.uuid",
        "wins_servers",
    ]
    """interfaces,mode,name,name_registration_type,node.name,node.uuid,scope,state,suffix,svm.name,svm.uuid,wins_servers,"""

    postable_fields = [
        "interfaces",
        "mode",
        "name",
        "name_registration_type",
        "node.name",
        "node.uuid",
        "scope",
        "state",
        "suffix",
        "svm.name",
        "svm.uuid",
        "wins_servers",
    ]
    """interfaces,mode,name,name_registration_type,node.name,node.uuid,scope,state,suffix,svm.name,svm.uuid,wins_servers,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in Netbios.get_collection(fields=field)]
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
            raise NetAppRestError("Netbios modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class Netbios(Resource):
    """Allows interaction with Netbios objects on the host"""

    _schema = NetbiosSchema
    _path = "/api/protocols/cifs/netbios"

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves NetBIOS information.
### Related ONTAP commands
* ` vserver cifs nbstat`
### Learn more
* [`DOC /protocols/cifs/netbios`](#docs-NAS-protocols_cifs_netbios)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="netbios show")
        def netbios_show(
            fields: List[Choices.define(["interfaces", "mode", "name", "name_registration_type", "scope", "state", "suffix", "time_left", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of Netbios resources

            Args:
                interfaces: 
                mode: Specifies the mode in which the NetBIOS name service is configured. The supported values are:   * p - Point to Point   * h - Hybrid   * m - Mixed   * b - Broadcast 
                name: Specifies the NetBIOS name.
                name_registration_type: Specifies the name registration type.
                scope: Specifies the NetBIOS name scope. Scope is used as a name for the set of NetBIOS nodes that participate in a NetBIOS over TCP (NBT) virtual LAN.
                state: Specifies the registration state of the NetBIOS Name.
                suffix: Specifies the NetBIOS suffix. A NetBIOS suffix is the 16th character of the 16-character NetBIOS name. The NetBIOS suffix is used by Microsoft Networking software to identify functionality installed on the registered device.
                time_left: Specifies the registration time left with WINS, in minutes.
            """

            kwargs = {}
            if interfaces is not None:
                kwargs["interfaces"] = interfaces
            if mode is not None:
                kwargs["mode"] = mode
            if name is not None:
                kwargs["name"] = name
            if name_registration_type is not None:
                kwargs["name_registration_type"] = name_registration_type
            if scope is not None:
                kwargs["scope"] = scope
            if state is not None:
                kwargs["state"] = state
            if suffix is not None:
                kwargs["suffix"] = suffix
            if time_left is not None:
                kwargs["time_left"] = time_left
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return Netbios.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all Netbios resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves NetBIOS information.
### Related ONTAP commands
* ` vserver cifs nbstat`
### Learn more
* [`DOC /protocols/cifs/netbios`](#docs-NAS-protocols_cifs_netbios)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)






