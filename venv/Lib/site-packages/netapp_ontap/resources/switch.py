r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API can be used to get information about the Ethernet switches used for cluster and/or storage networks. This API supports GET, PATCH, POST, and DELETE calls. The GET operation returns a list of discovered switches with status and configuration information. PATCH is used to modify the state of the switch. POST is used to add new switches. DELETE is used to remove existing switches.
## Examples
### Retrieving the ethernet switches for a cluster
The following example retrieves the ONTAP switches from the cluster.
Note that if the <i>fields=*</i> parameter is not specified, the fields snmp.version, snmp.user, version, monitoring.enabled, and monitoring.reason are not returned.
Filters can be added on the fields to limit the results.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Switch

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(Switch.get_collection(fields="*")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    Switch(
        {
            "network": "cluster",
            "model": "NX3232C",
            "_links": {
                "self": {
                    "href": "/api/network/ethernet/switches/RTP-CS01-510R11%28FOC22092K12%29"
                }
            },
            "serial_number": "Unknown",
            "address": "172.26.207.77",
            "snmp": {"user": "cshm1!", "version": "snmpv2c"},
            "name": "RTP-CS01-510R11(FOC22092K12)",
            "version": "Cisco Nexus Operating System (NX-OS) Software, Version 9.2(3)",
            "discovered": True,
            "monitoring": {"enabled": True, "monitored": True, "reason": "None"},
        }
    ),
    Switch(
        {
            "network": "cluster",
            "model": "NX3232C",
            "_links": {
                "self": {
                    "href": "/api/network/ethernet/switches/RTP-CS01-510R12%28FOC22373C3P%29"
                }
            },
            "serial_number": "FOC22373C3P",
            "address": "172.26.207.82",
            "snmp": {"user": "cshm1!", "version": "snmpv2c"},
            "name": "RTP-CS01-510R12(FOC22373C3P)",
            "version": "Cisco Nexus Operating System (NX-OS) Software, Version 9.2(3)",
            "discovered": True,
            "monitoring": {"enabled": True, "monitored": True, "reason": "None"},
        }
    ),
    Switch(
        {
            "network": "storage",
            "model": "NX3232C",
            "_links": {
                "self": {
                    "href": "/api/network/ethernet/switches/RTP-SS01-510R10%28FOC22170DFR%29"
                }
            },
            "serial_number": "FOC22170DFR",
            "address": "172.26.207.65",
            "snmp": {"user": "cshm1!", "version": "snmpv2c"},
            "name": "RTP-SS01-510R10(FOC22170DFR)",
            "version": "Cisco Nexus Operating System (NX-OS) Software, Version 9.3(3)",
            "discovered": True,
            "monitoring": {"enabled": True, "monitored": True, "reason": "None"},
        }
    ),
    Switch(
        {
            "network": "storage",
            "model": "NX3232C",
            "_links": {
                "self": {
                    "href": "/api/network/ethernet/switches/RTP-SS02-510R10%28FOC22131U6T%29"
                }
            },
            "serial_number": "FOC22131U6T",
            "address": "172.26.207.66",
            "snmp": {"user": "cshm1!", "version": "snmpv2c"},
            "name": "RTP-SS02-510R10(FOC22131U6T)",
            "version": "Cisco Nexus Operating System (NX-OS) Software, Version 9.3(3)",
            "discovered": True,
            "monitoring": {"enabled": True, "monitored": True, "reason": "None"},
        }
    ),
]

```
</div>
</div>

---
### Retrieving an ethernet switch for a cluster
The following example retrieves a single switch by name.
 ```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Switch

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Switch(name="RTP-SS02-510R10(FOC22131U6T)")
    resource.get(fields="*")
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
Switch(
    {
        "network": "storage",
        "model": "NX3232C",
        "_links": {
            "self": {
                "href": "/api/network/ethernet/switches/RTP-SS02-510R10(FOC22131U6T)"
            }
        },
        "serial_number": "FOC22131U6T",
        "address": "172.26.207.66",
        "snmp": {"user": "cshm1!", "version": "snmpv2c"},
        "name": "RTP-SS02-510R10(FOC22131U6T)",
        "version": "Cisco Nexus Operating System (NX-OS) Software, Version 9.3(3)",
        "discovered": True,
        "monitoring": {"enabled": True, "monitored": True, "reason": "None"},
    }
)

```
</div>
</div>

---
### Configuring a switch
The following example configures SNMP credential and version on a switch.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Switch

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Switch(name="sconqa-corduroyl-03")
    resource.snmp = {"version": "snmpv2c", "user": "cshm1!"}
    resource.patch()

```

---
### Adding a switch
The following example adds a switch.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Switch

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Switch()
    resource.name = "RTP-SS02-510R10(FOC22131U6T)"
    resource.address = "172.26.207.66"
    resource.model = "NX3232C"
    resource.monitoring = {"enabled": "true"}
    resource.network = "storage"
    resource.snmp = {"version": "snmpv2c", "user": "cshm1!"}
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example3_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example3_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example3_result" class="try_it_out_content">
```
Switch(
    {
        "network": "storage",
        "model": "NX3232C",
        "address": "172.26.207.66",
        "snmp": {"user": "cshm1!", "version": "snmpv2c"},
        "name": "RTP-SS02-510R10(FOC22131U6T)",
        "monitoring": {"enabled": True},
    }
)

```
</div>
</div>

---
### Deleting a switch
The following example deletes a switch.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Switch

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Switch(name="sconqa-corduroyl-03")
    resource.delete()

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


__all__ = ["Switch", "SwitchSchema"]
__pdoc__ = {
    "SwitchSchema.resource": False,
    "SwitchSchema.opts": False,
    "Switch.switch_show": False,
    "Switch.switch_create": False,
    "Switch.switch_modify": False,
    "Switch.switch_delete": False,
}


class SwitchSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Switch object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the switch."""

    address = fields.Str(
        data_key="address",
    )
    r""" IP Address."""

    discovered = fields.Boolean(
        data_key="discovered",
    )
    r""" Discovered By ONTAP CDP/LLDP"""

    model = fields.Str(
        data_key="model",
    )
    r""" Model Number."""

    monitoring = fields.Nested("netapp_ontap.models.switch_monitoring.SwitchMonitoringSchema", data_key="monitoring", unknown=EXCLUDE)
    r""" The monitoring field of the switch."""

    name = fields.Str(
        data_key="name",
    )
    r""" Name."""

    network = fields.Str(
        data_key="network",
        validate=enum_validation(['cluster', 'management', 'storage']),
    )
    r""" Switch Network.

Valid choices:

* cluster
* management
* storage"""

    serial_number = fields.Str(
        data_key="serial_number",
    )
    r""" Serial Number."""

    snmp = fields.Nested("netapp_ontap.models.switch_snmp.SwitchSnmpSchema", data_key="snmp", unknown=EXCLUDE)
    r""" The snmp field of the switch."""

    version = fields.Str(
        data_key="version",
    )
    r""" Software Version."""

    @property
    def resource(self):
        return Switch

    gettable_fields = [
        "links",
        "address",
        "discovered",
        "model",
        "monitoring",
        "name",
        "network",
        "serial_number",
        "snmp",
        "version",
    ]
    """links,address,discovered,model,monitoring,name,network,serial_number,snmp,version,"""

    patchable_fields = [
        "address",
        "monitoring",
        "snmp",
    ]
    """address,monitoring,snmp,"""

    postable_fields = [
        "address",
        "model",
        "monitoring",
        "name",
        "network",
        "snmp",
    ]
    """address,model,monitoring,name,network,snmp,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in Switch.get_collection(fields=field)]
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
            raise NetAppRestError("Switch modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class Switch(Resource):
    r""" Ethernet Switch REST API """

    _schema = SwitchSchema
    _path = "/api/network/ethernet/switches"
    _keys = ["name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the ethernet switches attached to the chassis.
### Related ONTAP commands
* `system switch ethernet show`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="switch show")
        def switch_show(
            fields: List[Choices.define(["address", "discovered", "model", "name", "network", "serial_number", "version", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of Switch resources

            Args:
                address: IP Address.
                discovered: Discovered By ONTAP CDP/LLDP
                model: Model Number.
                name: Name.
                network: Switch Network.
                serial_number: Serial Number.
                version: Software Version.
            """

            kwargs = {}
            if address is not None:
                kwargs["address"] = address
            if discovered is not None:
                kwargs["discovered"] = discovered
            if model is not None:
                kwargs["model"] = model
            if name is not None:
                kwargs["name"] = name
            if network is not None:
                kwargs["network"] = network
            if serial_number is not None:
                kwargs["serial_number"] = serial_number
            if version is not None:
                kwargs["version"] = version
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return Switch.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all Switch resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["Switch"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Update Ethernet Switch REST API
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["Switch"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["Switch"], NetAppResponse]:
        r"""Creates an ethernet switch.
### Required properties
* `name` - Name of the switch to create.
* `address` - Switch IP address.
* `model` - Switch model number.
* `monitoring.enabled` - Whether the switch should be monitored by CSHM.
* `network`
  * _cluster_ for cluster or shared switches.
  * _storage_ for storage switches.
  * _management_ for management switches.
* `snmp.version` - SNMP version.
* `snmp.user` - SNMP user.
### Related ONTAP commands
* `system switch ethernet create`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["Switch"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes an Ethernet switch.
### Related ONTAP commands
* `system switch ethernet delete`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the ethernet switches attached to the chassis.
### Related ONTAP commands
* `system switch ethernet show`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the details of an Ethernet switch.
### Related ONTAP commands
* `system switch ethernet show`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    def post(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Creates an ethernet switch.
### Required properties
* `name` - Name of the switch to create.
* `address` - Switch IP address.
* `model` - Switch model number.
* `monitoring.enabled` - Whether the switch should be monitored by CSHM.
* `network`
  * _cluster_ for cluster or shared switches.
  * _storage_ for storage switches.
  * _management_ for management switches.
* `snmp.version` - SNMP version.
* `snmp.user` - SNMP user.
### Related ONTAP commands
* `system switch ethernet create`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="switch create")
        async def switch_create(
        ) -> ResourceTable:
            """Create an instance of a Switch resource

            Args:
                links: 
                address: IP Address.
                discovered: Discovered By ONTAP CDP/LLDP
                model: Model Number.
                monitoring: 
                name: Name.
                network: Switch Network.
                serial_number: Serial Number.
                snmp: 
                version: Software Version.
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if address is not None:
                kwargs["address"] = address
            if discovered is not None:
                kwargs["discovered"] = discovered
            if model is not None:
                kwargs["model"] = model
            if monitoring is not None:
                kwargs["monitoring"] = monitoring
            if name is not None:
                kwargs["name"] = name
            if network is not None:
                kwargs["network"] = network
            if serial_number is not None:
                kwargs["serial_number"] = serial_number
            if snmp is not None:
                kwargs["snmp"] = snmp
            if version is not None:
                kwargs["version"] = version

            resource = Switch(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create Switch: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Update Ethernet Switch REST API
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="switch modify")
        async def switch_modify(
        ) -> ResourceTable:
            """Modify an instance of a Switch resource

            Args:
                address: IP Address.
                query_address: IP Address.
                discovered: Discovered By ONTAP CDP/LLDP
                query_discovered: Discovered By ONTAP CDP/LLDP
                model: Model Number.
                query_model: Model Number.
                name: Name.
                query_name: Name.
                network: Switch Network.
                query_network: Switch Network.
                serial_number: Serial Number.
                query_serial_number: Serial Number.
                version: Software Version.
                query_version: Software Version.
            """

            kwargs = {}
            changes = {}
            if query_address is not None:
                kwargs["address"] = query_address
            if query_discovered is not None:
                kwargs["discovered"] = query_discovered
            if query_model is not None:
                kwargs["model"] = query_model
            if query_name is not None:
                kwargs["name"] = query_name
            if query_network is not None:
                kwargs["network"] = query_network
            if query_serial_number is not None:
                kwargs["serial_number"] = query_serial_number
            if query_version is not None:
                kwargs["version"] = query_version

            if address is not None:
                changes["address"] = address
            if discovered is not None:
                changes["discovered"] = discovered
            if model is not None:
                changes["model"] = model
            if name is not None:
                changes["name"] = name
            if network is not None:
                changes["network"] = network
            if serial_number is not None:
                changes["serial_number"] = serial_number
            if version is not None:
                changes["version"] = version

            if hasattr(Switch, "find"):
                resource = Switch.find(
                    **kwargs
                )
            else:
                resource = Switch()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify Switch: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes an Ethernet switch.
### Related ONTAP commands
* `system switch ethernet delete`
### Learn more
* [`DOC /network/ethernet/switches`](#docs-networking-network_ethernet_switches)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="switch delete")
        async def switch_delete(
        ) -> None:
            """Delete an instance of a Switch resource

            Args:
                address: IP Address.
                discovered: Discovered By ONTAP CDP/LLDP
                model: Model Number.
                name: Name.
                network: Switch Network.
                serial_number: Serial Number.
                version: Software Version.
            """

            kwargs = {}
            if address is not None:
                kwargs["address"] = address
            if discovered is not None:
                kwargs["discovered"] = discovered
            if model is not None:
                kwargs["model"] = model
            if name is not None:
                kwargs["name"] = name
            if network is not None:
                kwargs["network"] = network
            if serial_number is not None:
                kwargs["serial_number"] = serial_number
            if version is not None:
                kwargs["version"] = version

            if hasattr(Switch, "find"):
                resource = Switch.find(
                    **kwargs
                )
            else:
                resource = Switch()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete Switch: %s" % err)


