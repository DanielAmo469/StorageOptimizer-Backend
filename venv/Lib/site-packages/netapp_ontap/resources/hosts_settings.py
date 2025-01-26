r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API is used to retrieve and manage hosts cache settings.
## Examples
### Retrieving hosts cache settings
---
The following examples shows how to use the cache host settings GET endpoint to retrieve host cache settings.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import HostsSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(HostsSettings.get_collection(fields="*")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    HostsSettings(
        {
            "dns_ttl_enabled": True,
            "ttl": "P1D",
            "svm": {"name": "vs43", "uuid": "8a1a8730-2036-11ec-8457-005056bbcfdb"},
            "negative_cache_enabled": True,
            "negative_ttl": "PT1M",
            "enabled": True,
        }
    ),
    HostsSettings(
        {
            "dns_ttl_enabled": True,
            "ttl": "P1D",
            "svm": {
                "name": "spragyaclus-2",
                "uuid": "951e8676-2035-11ec-bfe2-005056bb6bef",
            },
            "negative_cache_enabled": True,
            "negative_ttl": "PT1M",
            "enabled": True,
        }
    ),
    HostsSettings(
        {
            "dns_ttl_enabled": True,
            "ttl": "P1D",
            "svm": {"name": "vs34", "uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"},
            "negative_cache_enabled": True,
            "negative_ttl": "PT1M",
            "enabled": True,
        }
    ),
]

```
</div>
</div>

---
### Retrieving host cache settings for a given SVM
---
The following examples shows how to use the cache hosts settings GET endpoint to retrieve hosts cache settings for a specific SVM.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import HostsSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = HostsSettings(**{"svm.uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"})
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
HostsSettings(
    {
        "dns_ttl_enabled": True,
        "ttl": "P1D",
        "svm": {"name": "vs34", "uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"},
        "negative_cache_enabled": True,
        "negative_ttl": "PT1M",
        "enabled": False,
    }
)

```
</div>
</div>

---
### Updating a hosts cache setting
---
The following example shows how to use the cache host settings PATCH endpoint to update a host cache setting.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import HostsSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = HostsSettings(**{"svm.uuid": "02c9e252-41be-11e9-81d5-00a0986138f9"})
    resource.ttl = "PT2H"
    resource.negative_ttl = "PT2M"
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


__all__ = ["HostsSettings", "HostsSettingsSchema"]
__pdoc__ = {
    "HostsSettingsSchema.resource": False,
    "HostsSettingsSchema.opts": False,
    "HostsSettings.hosts_settings_show": False,
    "HostsSettings.hosts_settings_create": False,
    "HostsSettings.hosts_settings_modify": False,
    "HostsSettings.hosts_settings_delete": False,
}


class HostsSettingsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the HostsSettings object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the hosts_settings."""

    dns_ttl_enabled = fields.Boolean(
        data_key="dns_ttl_enabled",
    )
    r""" Specifies whether TTL is based on the DNS or host settings. If enabled, TTL from DNS is used.


Example: true"""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Indicates whether or not the cache is enabled."""

    negative_cache_enabled = fields.Boolean(
        data_key="negative_cache_enabled",
    )
    r""" Indicates whether or not the negative cache is enabled."""

    negative_ttl = fields.Str(
        data_key="negative_ttl",
    )
    r""" Specifies negative Time to Live, in ISO 8601 format.


Example: PT5M"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the hosts_settings."""

    ttl = fields.Str(
        data_key="ttl",
    )
    r""" Specifies Time to Live (TTL), in ISO 8601 format.


Example: PT24H"""

    @property
    def resource(self):
        return HostsSettings

    gettable_fields = [
        "links",
        "dns_ttl_enabled",
        "enabled",
        "negative_cache_enabled",
        "negative_ttl",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "ttl",
    ]
    """links,dns_ttl_enabled,enabled,negative_cache_enabled,negative_ttl,svm.links,svm.name,svm.uuid,ttl,"""

    patchable_fields = [
        "dns_ttl_enabled",
        "enabled",
        "negative_cache_enabled",
        "negative_ttl",
        "svm.name",
        "svm.uuid",
        "ttl",
    ]
    """dns_ttl_enabled,enabled,negative_cache_enabled,negative_ttl,svm.name,svm.uuid,ttl,"""

    postable_fields = [
        "dns_ttl_enabled",
        "enabled",
        "negative_cache_enabled",
        "negative_ttl",
        "svm.name",
        "svm.uuid",
        "ttl",
    ]
    """dns_ttl_enabled,enabled,negative_cache_enabled,negative_ttl,svm.name,svm.uuid,ttl,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in HostsSettings.get_collection(fields=field)]
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
            raise NetAppRestError("HostsSettings modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class HostsSettings(Resource):
    r""" Hosts cache setting. """

    _schema = HostsSettingsSchema
    _path = "/api/name-services/cache/host/settings"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves host cache settings.
### Related ONTAP commands
* `vserver services name-service cache hosts settings show`
### Learn more
* [`DOC /name-services/cache/host/settings`](#docs-name-services-name-services_cache_host_settings)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="hosts settings show")
        def hosts_settings_show(
            fields: List[Choices.define(["dns_ttl_enabled", "enabled", "negative_cache_enabled", "negative_ttl", "ttl", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of HostsSettings resources

            Args:
                dns_ttl_enabled: Specifies whether TTL is based on the DNS or host settings. If enabled, TTL from DNS is used. 
                enabled: Indicates whether or not the cache is enabled. 
                negative_cache_enabled: Indicates whether or not the negative cache is enabled. 
                negative_ttl: Specifies negative Time to Live, in ISO 8601 format. 
                ttl: Specifies Time to Live (TTL), in ISO 8601 format. 
            """

            kwargs = {}
            if dns_ttl_enabled is not None:
                kwargs["dns_ttl_enabled"] = dns_ttl_enabled
            if enabled is not None:
                kwargs["enabled"] = enabled
            if negative_cache_enabled is not None:
                kwargs["negative_cache_enabled"] = negative_cache_enabled
            if negative_ttl is not None:
                kwargs["negative_ttl"] = negative_ttl
            if ttl is not None:
                kwargs["ttl"] = ttl
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return HostsSettings.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all HostsSettings resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["HostsSettings"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a host cache setting.
### Important notes
  - svm.uuid field cannot be empty.
  - Returns success in case no values are provided for update.
### Related ONTAP commands
* `vserver services name-service cache hosts settings modify`
### Learn more
* [`DOC /name-services/cache/host/settings`](#docs-name-services-name-services_cache_host_settings)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves host cache settings.
### Related ONTAP commands
* `vserver services name-service cache hosts settings show`
### Learn more
* [`DOC /name-services/cache/host/settings`](#docs-name-services-name-services_cache_host_settings)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a host cache setting for a given SVM.
### Related ONTAP commands
* `vserver services name-service cache hosts settings show`
### Learn more
* [`DOC /name-services/cache/host/settings`](#docs-name-services-name-services_cache_host_settings)
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
        r"""Updates a host cache setting.
### Important notes
  - svm.uuid field cannot be empty.
  - Returns success in case no values are provided for update.
### Related ONTAP commands
* `vserver services name-service cache hosts settings modify`
### Learn more
* [`DOC /name-services/cache/host/settings`](#docs-name-services-name-services_cache_host_settings)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="hosts settings modify")
        async def hosts_settings_modify(
        ) -> ResourceTable:
            """Modify an instance of a HostsSettings resource

            Args:
                dns_ttl_enabled: Specifies whether TTL is based on the DNS or host settings. If enabled, TTL from DNS is used. 
                query_dns_ttl_enabled: Specifies whether TTL is based on the DNS or host settings. If enabled, TTL from DNS is used. 
                enabled: Indicates whether or not the cache is enabled. 
                query_enabled: Indicates whether or not the cache is enabled. 
                negative_cache_enabled: Indicates whether or not the negative cache is enabled. 
                query_negative_cache_enabled: Indicates whether or not the negative cache is enabled. 
                negative_ttl: Specifies negative Time to Live, in ISO 8601 format. 
                query_negative_ttl: Specifies negative Time to Live, in ISO 8601 format. 
                ttl: Specifies Time to Live (TTL), in ISO 8601 format. 
                query_ttl: Specifies Time to Live (TTL), in ISO 8601 format. 
            """

            kwargs = {}
            changes = {}
            if query_dns_ttl_enabled is not None:
                kwargs["dns_ttl_enabled"] = query_dns_ttl_enabled
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_negative_cache_enabled is not None:
                kwargs["negative_cache_enabled"] = query_negative_cache_enabled
            if query_negative_ttl is not None:
                kwargs["negative_ttl"] = query_negative_ttl
            if query_ttl is not None:
                kwargs["ttl"] = query_ttl

            if dns_ttl_enabled is not None:
                changes["dns_ttl_enabled"] = dns_ttl_enabled
            if enabled is not None:
                changes["enabled"] = enabled
            if negative_cache_enabled is not None:
                changes["negative_cache_enabled"] = negative_cache_enabled
            if negative_ttl is not None:
                changes["negative_ttl"] = negative_ttl
            if ttl is not None:
                changes["ttl"] = ttl

            if hasattr(HostsSettings, "find"):
                resource = HostsSettings.find(
                    **kwargs
                )
            else:
                resource = HostsSettings()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify HostsSettings: %s" % err)



