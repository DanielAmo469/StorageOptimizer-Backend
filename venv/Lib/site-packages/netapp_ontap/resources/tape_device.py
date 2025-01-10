r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Retrieving storage tape information
The storage tape GET API retrieves all of the tapes in the cluster.
<br/>
---
## Examples
### 1) Retrieving a list of tapes from the cluster
The following example returns the list of tapes in the cluster:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(TapeDevice.get_collection()))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    TapeDevice(
        {
            "device_id": "2d.0",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "2d.0L1",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "qeg-tape-brocade2-8g:0.126",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "stsw-broc6510-01:11.126",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "stsw-broc6510-01:15.126",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "stsw-broc6510-01:15.126L1",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "stsw-broc6510-01:22.126",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
    TapeDevice(
        {
            "device_id": "stsw-broc6510-01:23.126",
            "node": {
                "name": "st-8020-1-01",
                "_links": {
                    "self": {
                        "href": "/api/cluster/nodes/4083be52-5315-11eb-a839-00a0985ebbe7"
                    }
                },
                "uuid": "4083be52-5315-11eb-a839-00a0985ebbe7",
            },
        }
    ),
]

```
</div>
</div>

---
### 2) Retrieving a specific tape device from the cluster
The following example returns the requested tape device. If there is no tape with the requested UID, an error is returned.
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
TapeDevice(
    {
        "serial_number": "1068000245",
        "residual_count": 0,
        "device_state": "offline",
        "device_id": "2d.0",
        "wwnn": "5001697722ee0010",
        "alias": {"name": "st7", "mapping": "SN[1068000245]"},
        "reservation_type": "off",
        "file_number": -1,
        "interface": "sas",
        "wwpn": "5001697722ee0011",
        "description": "IBM LTO-6 ULT3580",
        "density": "low",
        "block_number": -1,
        "device_names": [
            {
                "rewind_device": "rst0l",
                "unload_reload_device": "urst0l",
                "no_rewind_device": "nrst0l",
            },
            {
                "rewind_device": "rst0m",
                "unload_reload_device": "urst0m",
                "no_rewind_device": "nrst0m",
            },
            {
                "rewind_device": "rst0h",
                "unload_reload_device": "urst0h",
                "no_rewind_device": "nrst0h",
            },
            {
                "rewind_device": "rst0a",
                "unload_reload_device": "urst0a",
                "no_rewind_device": "nrst0a",
            },
        ],
        "node": {
            "name": "st-8020-1-02",
            "_links": {
                "self": {
                    "href": "/api/cluster/nodes/5f5275eb-5315-11eb-8ac4-00a0985e0dcf"
                }
            },
            "uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf",
        },
        "storage_port": {"name": "2d"},
        "aliases": [{"name": "st7", "mapping": "SN[1068000245]"}],
        "type": "tape",
        "formats": [
            "LTO-4/5 Native Density",
            "LTO-4/5 Compressed",
            "LTO-6 2.5TB",
            "LTO-6 6.25TB Compressed",
        ],
    }
)

```
</div>
</div>

---
## Updating a tape device
The tape PATCH API allows the tape device to be set online or offline, positioned, and given an alias.
---
## Examples
### 1) Taking a tape device offline
The following example takes a tape device offline:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.online = False
    resource.patch()

```

---
### 2) Bringing a tape device online
The following example brings a tape device online:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.online = True
    resource.patch()

```

---
### 3) Giving a tape device an alias
The following example assigns an alias to a tape device:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.aliases = [{"name": "st0"}]
    resource.patch()

```

---
### 4) Removing a tape device's aliases
The following example clears any aliases previously assigned to a tape device:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.aliases = []
    resource.patch()

```

---
### 5) Rewinding a tape device
The following example rewinds a tape device:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.position = {"operation": "rewind"}
    resource.patch()

```

---
### 6) Forwarding the tape five files
The following example moves the tape forward five file records:
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import TapeDevice

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = TapeDevice(
        device_id="2d.0", **{"node.uuid": "5f5275eb-5315-11eb-8ac4-00a0985e0dcf"}
    )
    resource.position = {"operation": "fsf", "count": 5}
    resource.patch()

```

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


__all__ = ["TapeDevice", "TapeDeviceSchema"]
__pdoc__ = {
    "TapeDeviceSchema.resource": False,
    "TapeDeviceSchema.opts": False,
    "TapeDevice.tape_device_show": False,
    "TapeDevice.tape_device_create": False,
    "TapeDevice.tape_device_modify": False,
    "TapeDevice.tape_device_delete": False,
}


class TapeDeviceSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the TapeDevice object"""

    alias = fields.Nested("netapp_ontap.models.tape_device_alias.TapeDeviceAliasSchema", data_key="alias", unknown=EXCLUDE)
    r""" The alias field of the tape_device."""

    aliases = fields.List(fields.Nested("netapp_ontap.models.tape_device_aliases.TapeDeviceAliasesSchema", unknown=EXCLUDE), data_key="aliases")
    r""" The aliases field of the tape_device."""

    block_number = Size(
        data_key="block_number",
    )
    r""" Block number.

Example: 0"""

    density = fields.Str(
        data_key="density",
        validate=enum_validation(['low', 'medium', 'high', 'extended']),
    )
    r""" Density.

Valid choices:

* low
* medium
* high
* extended"""

    description = fields.Str(
        data_key="description",
    )
    r""" The description field of the tape_device.

Example: QUANTUM LTO-8 ULTRIUM"""

    device_id = fields.Str(
        data_key="device_id",
    )
    r""" The device_id field of the tape_device.

Example: 1a.0"""

    device_names = fields.List(fields.Nested("netapp_ontap.models.tape_device_device_names.TapeDeviceDeviceNamesSchema", unknown=EXCLUDE), data_key="device_names")
    r""" The device_names field of the tape_device."""

    device_state = fields.Str(
        data_key="device_state",
        validate=enum_validation(['unknown', 'available', 'ready_write_enabled', 'ready_write_protected', 'offline', 'in_use', 'error', 'reserved_by_another_host', 'normal', 'rewinding', 'erasing']),
    )
    r""" Operational state of the device.

Valid choices:

* unknown
* available
* ready_write_enabled
* ready_write_protected
* offline
* in_use
* error
* reserved_by_another_host
* normal
* rewinding
* erasing"""

    file_number = Size(
        data_key="file_number",
    )
    r""" File number.

Example: 0"""

    formats = fields.List(fields.Str, data_key="formats")
    r""" Tape cartridge format.

Example: ["LTO-7 6TB","LTO-7 15TB Compressed","LTO-8 12TB","LTO-8 30TB Compressed"]"""

    interface = fields.Str(
        data_key="interface",
        validate=enum_validation(['unknown', 'fibre_channel', 'sas', 'pscsi']),
    )
    r""" Device interface type.

Valid choices:

* unknown
* fibre_channel
* sas
* pscsi"""

    node = fields.Nested("netapp_ontap.resources.node.NodeSchema", data_key="node", unknown=EXCLUDE)
    r""" The node field of the tape_device."""

    online = fields.Boolean(
        data_key="online",
    )
    r""" The online field of the tape_device."""

    position = fields.Nested("netapp_ontap.models.tape_device_position.TapeDevicePositionSchema", data_key="position", unknown=EXCLUDE)
    r""" The position field of the tape_device."""

    reservation_type = fields.Str(
        data_key="reservation_type",
        validate=enum_validation(['off', 'persistent', 'scsi']),
    )
    r""" The reservation_type field of the tape_device.

Valid choices:

* off
* persistent
* scsi"""

    residual_count = Size(
        data_key="residual_count",
    )
    r""" Residual count of the last I/O operation.

Example: 0"""

    serial_number = fields.Str(
        data_key="serial_number",
    )
    r""" The serial_number field of the tape_device.

Example: 10WT00093"""

    storage_port = fields.Nested("netapp_ontap.models.tape_device_storage_port.TapeDeviceStoragePortSchema", data_key="storage_port", unknown=EXCLUDE)
    r""" The storage_port field of the tape_device."""

    type = fields.Str(
        data_key="type",
        validate=enum_validation(['unknown', 'tape', 'media_changer']),
    )
    r""" Device type.

Valid choices:

* unknown
* tape
* media_changer"""

    wwnn = fields.Str(
        data_key="wwnn",
    )
    r""" World Wide Node Name.

Example: 500507631295741c"""

    wwpn = fields.Str(
        data_key="wwpn",
    )
    r""" World Wide Port Name.

Example: 500507631295741c"""

    @property
    def resource(self):
        return TapeDevice

    gettable_fields = [
        "alias",
        "aliases",
        "block_number",
        "density",
        "description",
        "device_id",
        "device_names",
        "device_state",
        "file_number",
        "formats",
        "interface",
        "node.links",
        "node.name",
        "node.uuid",
        "online",
        "reservation_type",
        "residual_count",
        "serial_number",
        "storage_port",
        "type",
        "wwnn",
        "wwpn",
    ]
    """alias,aliases,block_number,density,description,device_id,device_names,device_state,file_number,formats,interface,node.links,node.name,node.uuid,online,reservation_type,residual_count,serial_number,storage_port,type,wwnn,wwpn,"""

    patchable_fields = [
        "aliases",
        "node.name",
        "node.uuid",
        "online",
        "position",
    ]
    """aliases,node.name,node.uuid,online,position,"""

    postable_fields = [
        "aliases",
        "node.name",
        "node.uuid",
        "online",
    ]
    """aliases,node.name,node.uuid,online,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in TapeDevice.get_collection(fields=field)]
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
            raise NetAppRestError("TapeDevice modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class TapeDevice(Resource):
    """Allows interaction with TapeDevice objects on the host"""

    _schema = TapeDeviceSchema
    _path = "/api/storage/tape-devices"
    _keys = ["node.uuid", "device_id"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves a collection of tape devices.
### Related ONTAP commands
* `storage tape show`
### Learn more
* [`DOC /storage/tape-devices`](#docs-storage-storage_tape-devices)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="tape device show")
        def tape_device_show(
            fields: List[Choices.define(["block_number", "density", "description", "device_id", "device_state", "file_number", "formats", "interface", "online", "reservation_type", "residual_count", "serial_number", "type", "wwnn", "wwpn", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of TapeDevice resources

            Args:
                block_number: Block number.
                density: Density.
                description: 
                device_id: 
                device_state: Operational state of the device.
                file_number: File number.
                formats: Tape cartridge format.
                interface: Device interface type.
                online: 
                reservation_type: 
                residual_count: Residual count of the last I/O operation.
                serial_number: 
                type: Device type.
                wwnn: World Wide Node Name.
                wwpn: World Wide Port Name.
            """

            kwargs = {}
            if block_number is not None:
                kwargs["block_number"] = block_number
            if density is not None:
                kwargs["density"] = density
            if description is not None:
                kwargs["description"] = description
            if device_id is not None:
                kwargs["device_id"] = device_id
            if device_state is not None:
                kwargs["device_state"] = device_state
            if file_number is not None:
                kwargs["file_number"] = file_number
            if formats is not None:
                kwargs["formats"] = formats
            if interface is not None:
                kwargs["interface"] = interface
            if online is not None:
                kwargs["online"] = online
            if reservation_type is not None:
                kwargs["reservation_type"] = reservation_type
            if residual_count is not None:
                kwargs["residual_count"] = residual_count
            if serial_number is not None:
                kwargs["serial_number"] = serial_number
            if type is not None:
                kwargs["type"] = type
            if wwnn is not None:
                kwargs["wwnn"] = wwnn
            if wwpn is not None:
                kwargs["wwpn"] = wwpn
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return TapeDevice.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all TapeDevice resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["TapeDevice"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a specific tape device.
### Related ONTAP commands
* `storage tape alias-set`
* `storage tape alias-clear`
* `storage tape online`
* `storage tape offline`
* `storage tape position`
### Learn more
* [`DOC /storage/tape-devices`](#docs-storage-storage_tape-devices)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves a collection of tape devices.
### Related ONTAP commands
* `storage tape show`
### Learn more
* [`DOC /storage/tape-devices`](#docs-storage-storage_tape-devices)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a specific tape device.
### Related ONTAP commands
* `storage tape show`
### Learn more
* [`DOC /storage/tape-devices`](#docs-storage-storage_tape-devices)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)


    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a specific tape device.
### Related ONTAP commands
* `storage tape alias-set`
* `storage tape alias-clear`
* `storage tape online`
* `storage tape offline`
* `storage tape position`
### Learn more
* [`DOC /storage/tape-devices`](#docs-storage-storage_tape-devices)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="tape device modify")
        async def tape_device_modify(
        ) -> ResourceTable:
            """Modify an instance of a TapeDevice resource

            Args:
                block_number: Block number.
                query_block_number: Block number.
                density: Density.
                query_density: Density.
                description: 
                query_description: 
                device_id: 
                query_device_id: 
                device_state: Operational state of the device.
                query_device_state: Operational state of the device.
                file_number: File number.
                query_file_number: File number.
                formats: Tape cartridge format.
                query_formats: Tape cartridge format.
                interface: Device interface type.
                query_interface: Device interface type.
                online: 
                query_online: 
                reservation_type: 
                query_reservation_type: 
                residual_count: Residual count of the last I/O operation.
                query_residual_count: Residual count of the last I/O operation.
                serial_number: 
                query_serial_number: 
                type: Device type.
                query_type: Device type.
                wwnn: World Wide Node Name.
                query_wwnn: World Wide Node Name.
                wwpn: World Wide Port Name.
                query_wwpn: World Wide Port Name.
            """

            kwargs = {}
            changes = {}
            if query_block_number is not None:
                kwargs["block_number"] = query_block_number
            if query_density is not None:
                kwargs["density"] = query_density
            if query_description is not None:
                kwargs["description"] = query_description
            if query_device_id is not None:
                kwargs["device_id"] = query_device_id
            if query_device_state is not None:
                kwargs["device_state"] = query_device_state
            if query_file_number is not None:
                kwargs["file_number"] = query_file_number
            if query_formats is not None:
                kwargs["formats"] = query_formats
            if query_interface is not None:
                kwargs["interface"] = query_interface
            if query_online is not None:
                kwargs["online"] = query_online
            if query_reservation_type is not None:
                kwargs["reservation_type"] = query_reservation_type
            if query_residual_count is not None:
                kwargs["residual_count"] = query_residual_count
            if query_serial_number is not None:
                kwargs["serial_number"] = query_serial_number
            if query_type is not None:
                kwargs["type"] = query_type
            if query_wwnn is not None:
                kwargs["wwnn"] = query_wwnn
            if query_wwpn is not None:
                kwargs["wwpn"] = query_wwpn

            if block_number is not None:
                changes["block_number"] = block_number
            if density is not None:
                changes["density"] = density
            if description is not None:
                changes["description"] = description
            if device_id is not None:
                changes["device_id"] = device_id
            if device_state is not None:
                changes["device_state"] = device_state
            if file_number is not None:
                changes["file_number"] = file_number
            if formats is not None:
                changes["formats"] = formats
            if interface is not None:
                changes["interface"] = interface
            if online is not None:
                changes["online"] = online
            if reservation_type is not None:
                changes["reservation_type"] = reservation_type
            if residual_count is not None:
                changes["residual_count"] = residual_count
            if serial_number is not None:
                changes["serial_number"] = serial_number
            if type is not None:
                changes["type"] = type
            if wwnn is not None:
                changes["wwnn"] = wwnn
            if wwpn is not None:
                changes["wwpn"] = wwpn

            if hasattr(TapeDevice, "find"):
                resource = TapeDevice.find(
                    **kwargs
                )
            else:
                resource = TapeDevice()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify TapeDevice: %s" % err)



