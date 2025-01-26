r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
Displays CIFS domain-related information of the specified SVM.
## Examples
### Retrieving all the fields of CIFS domain configurations of a specific SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsDomain

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = CifsDomain(**{"svm.uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"})
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
CifsDomain(
    {
        "name_mapping": {"trusted_domains": ["SERVER03.COM", "SERVER04.COM"]},
        "svm": {"name": "vs2", "uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"},
        "trust_relationships": [
            {
                "home_domain": "SERVER02.COM",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "trusted_domains": ["SERVER02.COM"],
            }
        ],
        "password_schedule": {
            "schedule_enabled": False,
            "schedule_description": "Tue@1:00",
            "schedule_randomized_minute": 120,
            "schedule_weekly_interval": 4,
        },
        "server_discovery_mode": "all",
        "preferred_dcs": [{"server_ip": "192.168.20.1", "fqdn": "server02.com"}],
        "discovered_servers": [
            {
                "server_name": "scspb0659002001",
                "server_ip": "192.168.20.1",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "state": "undetermined",
                "preference": "preferred",
                "server_type": "kerberos",
                "domain": "server02.com",
            },
            {
                "server_name": "scspb0659002001",
                "server_ip": "192.168.20.1",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "state": "undetermined",
                "preference": "preferred",
                "server_type": "ms_ldap",
                "domain": "server02.com",
            },
            {
                "server_name": "scspb0659002001",
                "server_ip": "192.168.20.1",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "state": "undetermined",
                "preference": "preferred",
                "server_type": "ms_dc",
                "domain": "server02.com",
            },
        ],
    }
)

```
</div>
</div>

---
### Applying rediscover_trusts query parameter and retrieving all the fields of CIFS domain configurations
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsDomain

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = CifsDomain(**{"svm.uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"})
    resource.get(rediscover_trusts=True)
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
CifsDomain(
    {
        "name_mapping": {"trusted_domains": ["SERVER03.COM", "SERVER04.COM"]},
        "svm": {"name": "vs2", "uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"},
        "trust_relationships": [
            {
                "home_domain": "SERVER02.COM",
                "node": {
                    "name": "vsNode1",
                    "uuid": "a64c0906-c7dd-11eb-af15-0050568e403e",
                },
                "trusted_domains": ["SERVER02.COM"],
            },
            {
                "home_domain": "SERVER02.COM",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "trusted_domains": ["SERVER02.COM"],
            },
        ],
        "password_schedule": {
            "schedule_enabled": False,
            "schedule_description": "Tue@1:00",
            "schedule_randomized_minute": 120,
            "schedule_weekly_interval": 4,
        },
        "server_discovery_mode": "all",
        "preferred_dcs": [{"server_ip": "192.168.20.1", "fqdn": "server02.com"}],
        "discovered_servers": [
            {
                "server_name": "scspb0659002001",
                "server_ip": "192.168.20.1",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "state": "undetermined",
                "preference": "preferred",
                "server_type": "kerberos",
                "domain": "server02.com",
            },
            {
                "server_name": "scspb0659002001",
                "server_ip": "192.168.20.1",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "state": "undetermined",
                "preference": "preferred",
                "server_type": "ms_ldap",
                "domain": "server02.com",
            },
            {
                "server_name": "scspb0659002001",
                "server_ip": "192.168.20.1",
                "node": {
                    "name": "vsNode2",
                    "uuid": "4d9400f0-c84b-11eb-90ab-0050568e7324",
                },
                "state": "undetermined",
                "preference": "preferred",
                "server_type": "ms_dc",
                "domain": "server02.com",
            },
        ],
    }
)

```
</div>
</div>

---
### Modifying the Password Schedule of CIFS domain configurations of a specific SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsDomain

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = CifsDomain(**{"svm.uuid": "a83f816a-c873-11ed-a3b2-0050568e278e"})
    resource.password_schedule = {"schedule_enabled": "true"}
    resource.patch()

```

---
### Modifying the discovery-mode of CIFS domain configurations of a specific SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import CifsDomain

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = CifsDomain(**{"svm.uuid": "a83f816a-c873-11ed-a3b2-0050568e278e"})
    resource.server_discovery_mode = "site"
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


__all__ = ["CifsDomain", "CifsDomainSchema"]
__pdoc__ = {
    "CifsDomainSchema.resource": False,
    "CifsDomainSchema.opts": False,
    "CifsDomain.cifs_domain_show": False,
    "CifsDomain.cifs_domain_create": False,
    "CifsDomain.cifs_domain_modify": False,
    "CifsDomain.cifs_domain_delete": False,
}


class CifsDomainSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the CifsDomain object"""

    discovered_servers = fields.List(fields.Nested("netapp_ontap.models.cifs_domain_discovered_server.CifsDomainDiscoveredServerSchema", unknown=EXCLUDE), data_key="discovered_servers")
    r""" Specifies the discovered servers records."""

    name_mapping = fields.Nested("netapp_ontap.models.cifs_domain_name_mapping.CifsDomainNameMappingSchema", data_key="name_mapping", unknown=EXCLUDE)
    r""" The name_mapping field of the cifs_domain."""

    password_schedule = fields.Nested("netapp_ontap.models.cifs_domain_password_schedule.CifsDomainPasswordScheduleSchema", data_key="password_schedule", unknown=EXCLUDE)
    r""" The password_schedule field of the cifs_domain."""

    preferred_dcs = fields.List(fields.Nested("netapp_ontap.resources.cifs_domain_preferred_dc.CifsDomainPreferredDcSchema", unknown=EXCLUDE), data_key="preferred_dcs")
    r""" Specifies the preferred DC records."""

    server_discovery_mode = fields.Str(
        data_key="server_discovery_mode",
        validate=enum_validation(['all', 'site', 'none']),
    )
    r""" Specifies the mode of server discovery.


Valid choices:

* all
* site
* none"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the cifs_domain."""

    trust_relationships = fields.List(fields.Nested("netapp_ontap.models.cifs_domain_trust.CifsDomainTrustSchema", unknown=EXCLUDE), data_key="trust_relationships")
    r""" Specifies the trusted domain records."""

    @property
    def resource(self):
        return CifsDomain

    gettable_fields = [
        "discovered_servers",
        "name_mapping",
        "password_schedule",
        "preferred_dcs",
        "server_discovery_mode",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "trust_relationships",
    ]
    """discovered_servers,name_mapping,password_schedule,preferred_dcs,server_discovery_mode,svm.links,svm.name,svm.uuid,trust_relationships,"""

    patchable_fields = [
        "name_mapping",
        "password_schedule",
        "server_discovery_mode",
        "svm.name",
        "svm.uuid",
    ]
    """name_mapping,password_schedule,server_discovery_mode,svm.name,svm.uuid,"""

    postable_fields = [
        "name_mapping",
        "password_schedule",
        "server_discovery_mode",
        "svm.name",
        "svm.uuid",
    ]
    """name_mapping,password_schedule,server_discovery_mode,svm.name,svm.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in CifsDomain.get_collection(fields=field)]
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
            raise NetAppRestError("CifsDomain modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class CifsDomain(Resource):
    """Allows interaction with CifsDomain objects on the host"""

    _schema = CifsDomainSchema
    _path = "/api/protocols/cifs/domains"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the CIFS domain-related information of all SVMs.
### Related ONTAP commands
* `vserver cifs domain preferred-dc show`
* `vserver cifs domain trusts show`
* `vserver cifs domain discovered-servers show`
* `vserver cifs domain name-mapping-search show`
* `vserver cifs domain schedule show`
### Learn more
* [`DOC /protocols/cifs/domains`](#docs-NAS-protocols_cifs_domains)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="cifs domain show")
        def cifs_domain_show(
            fields: List[Choices.define(["server_discovery_mode", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of CifsDomain resources

            Args:
                server_discovery_mode: Specifies the mode of server discovery. 
            """

            kwargs = {}
            if server_discovery_mode is not None:
                kwargs["server_discovery_mode"] = server_discovery_mode
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return CifsDomain.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all CifsDomain resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["CifsDomain"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Modifies the CIFS domain-related information of the specified SVM.
### Related ONTAP commands
* `vserver cifs domain password schedule modify`
* `vserver cifs domain discovered-servers discovery-mode modify`
### Learn more
* [`DOC /protocols/cifs/domains/{svm.uuid}`](#docs-NAS-protocols_cifs_domains_{svm.uuid})
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the CIFS domain-related information of all SVMs.
### Related ONTAP commands
* `vserver cifs domain preferred-dc show`
* `vserver cifs domain trusts show`
* `vserver cifs domain discovered-servers show`
* `vserver cifs domain name-mapping-search show`
* `vserver cifs domain schedule show`
### Learn more
* [`DOC /protocols/cifs/domains`](#docs-NAS-protocols_cifs_domains)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the CIFS domain-related information of the specified SVM.
### Important notes
GET operation with query parameter `rediscover_trusts` and `reset_discovered_servers` returns available CIFS domain configurations and also triggers trusts rediscovery and discovered servers reset asynchronously for that SVM.
### Related ONTAP commands
* `vserver cifs domain preferred-dc show`
* `vserver cifs domain trusts show`
* `vserver cifs domain discovered-servers show`
* `vserver cifs domain name-mapping-search show`
* `vserver cifs domain password schedule show`
* `vserver cifs domain discovered-servers discovery-mode show`
### Learn more
* [`DOC /protocols/cifs/domains/{svm.uuid}`](#docs-NAS-protocols_cifs_domains_{svm.uuid})
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
        r"""Modifies the CIFS domain-related information of the specified SVM.
### Related ONTAP commands
* `vserver cifs domain password schedule modify`
* `vserver cifs domain discovered-servers discovery-mode modify`
### Learn more
* [`DOC /protocols/cifs/domains/{svm.uuid}`](#docs-NAS-protocols_cifs_domains_{svm.uuid})
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="cifs domain modify")
        async def cifs_domain_modify(
        ) -> ResourceTable:
            """Modify an instance of a CifsDomain resource

            Args:
                server_discovery_mode: Specifies the mode of server discovery. 
                query_server_discovery_mode: Specifies the mode of server discovery. 
            """

            kwargs = {}
            changes = {}
            if query_server_discovery_mode is not None:
                kwargs["server_discovery_mode"] = query_server_discovery_mode

            if server_discovery_mode is not None:
                changes["server_discovery_mode"] = server_discovery_mode

            if hasattr(CifsDomain, "find"):
                resource = CifsDomain.find(
                    **kwargs
                )
            else:
                resource = CifsDomain()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify CifsDomain: %s" % err)



