r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API is used to retrieve and manage global nameservice cache settings.
## Examples
### Retrieving a global nameservice cache setting
---
The following example shows how to use the cache setting GET endpoint to retrieve the global nameservice cache setting.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import GlobalCacheSetting

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = GlobalCacheSetting()
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
GlobalCacheSetting({"eviction_time_interval": "P2D", "remote_fetch_enabled": True})

```
</div>
</div>

---
### Updating a global nameservice cache setting
---
The following example shows how to use the cache setting PATCH endpoint to update the global nameservice cache setting.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import GlobalCacheSetting

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = GlobalCacheSetting()
    resource.eviction_time_interval = "PT2H"
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


__all__ = ["GlobalCacheSetting", "GlobalCacheSettingSchema"]
__pdoc__ = {
    "GlobalCacheSettingSchema.resource": False,
    "GlobalCacheSettingSchema.opts": False,
    "GlobalCacheSetting.global_cache_setting_show": False,
    "GlobalCacheSetting.global_cache_setting_create": False,
    "GlobalCacheSetting.global_cache_setting_modify": False,
    "GlobalCacheSetting.global_cache_setting_delete": False,
}


class GlobalCacheSettingSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GlobalCacheSetting object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the global_cache_setting."""

    eviction_time_interval = fields.Str(
        data_key="eviction_time_interval",
    )
    r""" Specifies the time interval, in ISO 8601 format after which a periodic cache eviction happens. Default is 4 hours.


Example: PT2H"""

    remote_fetch_enabled = fields.Boolean(
        data_key="remote_fetch_enabled",
    )
    r""" Indicates whether or not a node is allowed to fetch the data from a remote node."""

    @property
    def resource(self):
        return GlobalCacheSetting

    gettable_fields = [
        "links",
        "eviction_time_interval",
        "remote_fetch_enabled",
    ]
    """links,eviction_time_interval,remote_fetch_enabled,"""

    patchable_fields = [
        "eviction_time_interval",
        "remote_fetch_enabled",
    ]
    """eviction_time_interval,remote_fetch_enabled,"""

    postable_fields = [
        "eviction_time_interval",
        "remote_fetch_enabled",
    ]
    """eviction_time_interval,remote_fetch_enabled,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in GlobalCacheSetting.get_collection(fields=field)]
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
            raise NetAppRestError("GlobalCacheSetting modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class GlobalCacheSetting(Resource):
    """Allows interaction with GlobalCacheSetting objects on the host"""

    _schema = GlobalCacheSettingSchema
    _path = "/api/name-services/cache/setting"






    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a global nameservice cache setting.
### Related ONTAP commands
* `vserver services name-service cache settings show`
### Learn more
* [`DOC /name-services/cache/setting`](#docs-name-services-name-services_cache_setting)
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="global cache setting show")
        def global_cache_setting_show(
            fields: List[str] = None,
        ) -> ResourceTable:
            """Fetch a single GlobalCacheSetting resource

            Args:
                eviction_time_interval: Specifies the time interval, in ISO 8601 format after which a periodic cache eviction happens. Default is 4 hours. 
                remote_fetch_enabled: Indicates whether or not a node is allowed to fetch the data from a remote node. 
            """

            kwargs = {}
            if eviction_time_interval is not None:
                kwargs["eviction_time_interval"] = eviction_time_interval
            if remote_fetch_enabled is not None:
                kwargs["remote_fetch_enabled"] = remote_fetch_enabled
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            resource = GlobalCacheSetting(
                **kwargs
            )
            resource.get()
            return [resource]


    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a global nameservice cache setting.
### Important notes
  - Both the cache eviction time and remote fetch option can be modified.
### Related ONTAP commands
* `vserver services name-service cache settings modify`
### Learn more
* [`DOC /name-services/cache/setting`](#docs-name-services-name-services_cache_setting)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="global cache setting modify")
        async def global_cache_setting_modify(
        ) -> ResourceTable:
            """Modify an instance of a GlobalCacheSetting resource

            Args:
                eviction_time_interval: Specifies the time interval, in ISO 8601 format after which a periodic cache eviction happens. Default is 4 hours. 
                query_eviction_time_interval: Specifies the time interval, in ISO 8601 format after which a periodic cache eviction happens. Default is 4 hours. 
                remote_fetch_enabled: Indicates whether or not a node is allowed to fetch the data from a remote node. 
                query_remote_fetch_enabled: Indicates whether or not a node is allowed to fetch the data from a remote node. 
            """

            kwargs = {}
            changes = {}
            if query_eviction_time_interval is not None:
                kwargs["eviction_time_interval"] = query_eviction_time_interval
            if query_remote_fetch_enabled is not None:
                kwargs["remote_fetch_enabled"] = query_remote_fetch_enabled

            if eviction_time_interval is not None:
                changes["eviction_time_interval"] = eviction_time_interval
            if remote_fetch_enabled is not None:
                changes["remote_fetch_enabled"] = remote_fetch_enabled

            if hasattr(GlobalCacheSetting, "find"):
                resource = GlobalCacheSetting.find(
                    **kwargs
                )
            else:
                resource = GlobalCacheSetting()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify GlobalCacheSetting: %s" % err)



