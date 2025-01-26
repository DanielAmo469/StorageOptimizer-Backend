r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API is used to retrieve and manage netgroups cache settings.
## Examples
### Retrieving a netgroup cache setting
---
The following example shows how to use the cache netgroup settings GET endpoint to retrieve netgroup cache settings.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NetgroupsSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(NetgroupsSettings.get_collection(fields="*")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    NetgroupsSettings(
        {
            "negative_ttl_byhost": "PT30M",
            "negative_cache_enabled_byhost": True,
            "svm": {"name": "vs43", "uuid": "8a1a8730-2036-11ec-8457-005056bbcfdb"},
            "ttl_for_members": "PT30M",
            "ttl_byhost": "P1D",
            "enabled": True,
        }
    ),
    NetgroupsSettings(
        {
            "negative_ttl_byhost": "PT30M",
            "negative_cache_enabled_byhost": True,
            "svm": {"name": "vs34", "uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"},
            "ttl_for_members": "PT30M",
            "ttl_byhost": "PT2H",
            "enabled": True,
        }
    ),
]

```
</div>
</div>

---
### Retrieving a netgroup cache setting for a given SVM
---
The following example shows how to use the cache netgroup settings GET endpoint to retrieve netgroup cache settings for a given SVM.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NetgroupsSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = NetgroupsSettings(**{"svm.uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"})
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
NetgroupsSettings(
    {
        "negative_ttl_byhost": "PT30M",
        "negative_cache_enabled_byhost": True,
        "svm": {"name": "vs34", "uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"},
        "ttl_for_members": "PT30M",
        "ttl_byhost": "P1D",
        "enabled": True,
    }
)

```
</div>
</div>

---
### Updating a netgroup cache setting
---
The following example shows how to use the cache netgroup settings PATCH endpoint to update netgroup cache settings.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NetgroupsSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = NetgroupsSettings(**{"svm.uuid": "02c9e252-41be-11e9-81d5-00a0986138f9"})
    resource.ttl_byhost = "PT2H"
    resource.negative_ttl_byhost = "PT2M"
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


__all__ = ["NetgroupsSettings", "NetgroupsSettingsSchema"]
__pdoc__ = {
    "NetgroupsSettingsSchema.resource": False,
    "NetgroupsSettingsSchema.opts": False,
    "NetgroupsSettings.netgroups_settings_show": False,
    "NetgroupsSettings.netgroups_settings_create": False,
    "NetgroupsSettings.netgroups_settings_modify": False,
    "NetgroupsSettings.netgroups_settings_delete": False,
}


class NetgroupsSettingsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NetgroupsSettings object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the netgroups_settings."""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Indicates whether or not the cache is enabled."""

    negative_cache_enabled_byhost = fields.Boolean(
        data_key="negative_cache_enabled_byhost",
    )
    r""" Indicates whether or not the negative cache is enabled."""

    negative_ttl_byhost = fields.Str(
        data_key="negative_ttl_byhost",
    )
    r""" Specifies negative Time to Live by host, in ISO 8601 format.


Example: PT2M5S"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the netgroups_settings."""

    ttl_byhost = fields.Str(
        data_key="ttl_byhost",
    )
    r""" Specifies Time to Live by host, in ISO 8601 format.


Example: PT24H"""

    ttl_for_members = fields.Str(
        data_key="ttl_for_members",
    )
    r""" Specifies Time to Live for netgroup members, in ISO 8601 format.


Example: PT2M"""

    @property
    def resource(self):
        return NetgroupsSettings

    gettable_fields = [
        "links",
        "enabled",
        "negative_cache_enabled_byhost",
        "negative_ttl_byhost",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "ttl_byhost",
        "ttl_for_members",
    ]
    """links,enabled,negative_cache_enabled_byhost,negative_ttl_byhost,svm.links,svm.name,svm.uuid,ttl_byhost,ttl_for_members,"""

    patchable_fields = [
        "enabled",
        "negative_cache_enabled_byhost",
        "negative_ttl_byhost",
        "svm.name",
        "svm.uuid",
        "ttl_byhost",
        "ttl_for_members",
    ]
    """enabled,negative_cache_enabled_byhost,negative_ttl_byhost,svm.name,svm.uuid,ttl_byhost,ttl_for_members,"""

    postable_fields = [
        "enabled",
        "negative_cache_enabled_byhost",
        "negative_ttl_byhost",
        "svm.name",
        "svm.uuid",
        "ttl_byhost",
        "ttl_for_members",
    ]
    """enabled,negative_cache_enabled_byhost,negative_ttl_byhost,svm.name,svm.uuid,ttl_byhost,ttl_for_members,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in NetgroupsSettings.get_collection(fields=field)]
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
            raise NetAppRestError("NetgroupsSettings modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class NetgroupsSettings(Resource):
    r""" Netgroups cache setting. """

    _schema = NetgroupsSettingsSchema
    _path = "/api/name-services/cache/netgroup/settings"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves netgroups cache settings.
### Related ONTAP commands
* `vserver services name-service cache netgroups settings show`
### Learn more
* [`DOC /name-services/cache/netgroup/settings`](#docs-name-services-name-services_cache_netgroup_settings)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="netgroups settings show")
        def netgroups_settings_show(
            fields: List[Choices.define(["enabled", "negative_cache_enabled_byhost", "negative_ttl_byhost", "ttl_byhost", "ttl_for_members", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of NetgroupsSettings resources

            Args:
                enabled: Indicates whether or not the cache is enabled. 
                negative_cache_enabled_byhost: Indicates whether or not the negative cache is enabled. 
                negative_ttl_byhost: Specifies negative Time to Live by host, in ISO 8601 format. 
                ttl_byhost: Specifies Time to Live by host, in ISO 8601 format. 
                ttl_for_members: Specifies Time to Live for netgroup members, in ISO 8601 format. 
            """

            kwargs = {}
            if enabled is not None:
                kwargs["enabled"] = enabled
            if negative_cache_enabled_byhost is not None:
                kwargs["negative_cache_enabled_byhost"] = negative_cache_enabled_byhost
            if negative_ttl_byhost is not None:
                kwargs["negative_ttl_byhost"] = negative_ttl_byhost
            if ttl_byhost is not None:
                kwargs["ttl_byhost"] = ttl_byhost
            if ttl_for_members is not None:
                kwargs["ttl_for_members"] = ttl_for_members
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return NetgroupsSettings.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all NetgroupsSettings resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["NetgroupsSettings"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a netgroup cache setting.
### Important notes
  - svm.uuid field cannot be empty.
  - Returns success in case no values are provided for update.
### Related ONTAP commands
* `vserver services name-service cache netgroups settings modify`
### Learn more
* [`DOC /name-services/cache/netgroup/settings`](#docs-name-services-name-services_cache_netgroup_settings)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves netgroups cache settings.
### Related ONTAP commands
* `vserver services name-service cache netgroups settings show`
### Learn more
* [`DOC /name-services/cache/netgroup/settings`](#docs-name-services-name-services_cache_netgroup_settings)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a netgroup cache setting for a given SVM.
### Related ONTAP commands
* `vserver services name-service cache netgroups settings show`
### Learn more
* [`DOC /name-services/cache/netgroup/settings`](#docs-name-services-name-services_cache_netgroup_settings)
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
        r"""Updates a netgroup cache setting.
### Important notes
  - svm.uuid field cannot be empty.
  - Returns success in case no values are provided for update.
### Related ONTAP commands
* `vserver services name-service cache netgroups settings modify`
### Learn more
* [`DOC /name-services/cache/netgroup/settings`](#docs-name-services-name-services_cache_netgroup_settings)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="netgroups settings modify")
        async def netgroups_settings_modify(
        ) -> ResourceTable:
            """Modify an instance of a NetgroupsSettings resource

            Args:
                enabled: Indicates whether or not the cache is enabled. 
                query_enabled: Indicates whether or not the cache is enabled. 
                negative_cache_enabled_byhost: Indicates whether or not the negative cache is enabled. 
                query_negative_cache_enabled_byhost: Indicates whether or not the negative cache is enabled. 
                negative_ttl_byhost: Specifies negative Time to Live by host, in ISO 8601 format. 
                query_negative_ttl_byhost: Specifies negative Time to Live by host, in ISO 8601 format. 
                ttl_byhost: Specifies Time to Live by host, in ISO 8601 format. 
                query_ttl_byhost: Specifies Time to Live by host, in ISO 8601 format. 
                ttl_for_members: Specifies Time to Live for netgroup members, in ISO 8601 format. 
                query_ttl_for_members: Specifies Time to Live for netgroup members, in ISO 8601 format. 
            """

            kwargs = {}
            changes = {}
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_negative_cache_enabled_byhost is not None:
                kwargs["negative_cache_enabled_byhost"] = query_negative_cache_enabled_byhost
            if query_negative_ttl_byhost is not None:
                kwargs["negative_ttl_byhost"] = query_negative_ttl_byhost
            if query_ttl_byhost is not None:
                kwargs["ttl_byhost"] = query_ttl_byhost
            if query_ttl_for_members is not None:
                kwargs["ttl_for_members"] = query_ttl_for_members

            if enabled is not None:
                changes["enabled"] = enabled
            if negative_cache_enabled_byhost is not None:
                changes["negative_cache_enabled_byhost"] = negative_cache_enabled_byhost
            if negative_ttl_byhost is not None:
                changes["negative_ttl_byhost"] = negative_ttl_byhost
            if ttl_byhost is not None:
                changes["ttl_byhost"] = ttl_byhost
            if ttl_for_members is not None:
                changes["ttl_for_members"] = ttl_for_members

            if hasattr(NetgroupsSettings, "find"):
                resource = NetgroupsSettings.find(
                    **kwargs
                )
            else:
                resource = NetgroupsSettings()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify NetgroupsSettings: %s" % err)



