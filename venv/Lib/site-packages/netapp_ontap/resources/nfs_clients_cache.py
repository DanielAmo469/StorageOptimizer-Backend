r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This cluster-wide API is used to set the maximum cache idle time (client_retention_interval) for the connected-clients cache. If a
client connected to NFS server is idle for longer than than the maximum cache idle time, the entry will be removed. The update_interval
value will change when the client_retention_interval is changed. The update interval represents the interval between the cleaning
happens. If the value of client_retention_interval is set to 60hrs the connected client entry will stay there for 60 hours
and after that it will get removed. If the value of update_interval is 8 hours then the cache will be refreshed once every 8 hours.<p/>
## Example
### Retrieves connected-client cache settings information
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsClientsCache

with HostConnection(
    "<cluster-mgmt-ip>", username="admin", password="password", verify=False
):
    resource = NfsClientsCache()
    resource.get(return_timeout=15)
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
NfsClientsCache({"client_retention_interval": "P7D", "update_interval": "PT8H"})

```
</div>
</div>

### Updating connected-client cache settings
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import NfsClientsCache

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = NfsClientsCache()
    resource.client_retention_interval = "P7D"
    resource.patch()

```
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


__all__ = ["NfsClientsCache", "NfsClientsCacheSchema"]
__pdoc__ = {
    "NfsClientsCacheSchema.resource": False,
    "NfsClientsCacheSchema.opts": False,
    "NfsClientsCache.nfs_clients_cache_show": False,
    "NfsClientsCache.nfs_clients_cache_create": False,
    "NfsClientsCache.nfs_clients_cache_modify": False,
    "NfsClientsCache.nfs_clients_cache_delete": False,
}


class NfsClientsCacheSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the NfsClientsCache object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the nfs_clients_cache."""

    client_retention_interval = fields.Str(
        data_key="client_retention_interval",
    )
    r""" The lifetime range of the connected-clients cache. Only intervals in multiples of 12 hours or its equivalent in days, minutes or seconds are supported. The minimum is 12 hours and the maximum is 168 hours or 7 days."""

    enable_nfs_clients_deletion = fields.Boolean(
        data_key="enable_nfs_clients_deletion",
    )
    r""" Specifies whether or not NFS Clients deletion is enabled for the connected-clients cache. When set to "true", connected-clients entries are deleted when a connection is closed."""

    update_interval = fields.Str(
        data_key="update_interval",
    )
    r""" The time interval between refreshing the connected-clients cache. The minimum is 1 hour and the maximum is 8 hours."""

    @property
    def resource(self):
        return NfsClientsCache

    gettable_fields = [
        "links",
        "client_retention_interval",
        "enable_nfs_clients_deletion",
        "update_interval",
    ]
    """links,client_retention_interval,enable_nfs_clients_deletion,update_interval,"""

    patchable_fields = [
        "client_retention_interval",
        "enable_nfs_clients_deletion",
    ]
    """client_retention_interval,enable_nfs_clients_deletion,"""

    postable_fields = [
        "client_retention_interval",
        "enable_nfs_clients_deletion",
    ]
    """client_retention_interval,enable_nfs_clients_deletion,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in NfsClientsCache.get_collection(fields=field)]
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
            raise NetAppRestError("NfsClientsCache modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class NfsClientsCache(Resource):
    """Allows interaction with NfsClientsCache objects on the host"""

    _schema = NfsClientsCacheSchema
    _path = "/api/protocols/nfs/connected-client-settings"






    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the NFS connected-client cache settings of the cluster.

### Learn more
* [`DOC /protocols/nfs/connected-client-settings`](#docs-NAS-protocols_nfs_connected-client-settings)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs clients cache show")
        def nfs_clients_cache_show(
            fields: List[str] = None,
        ) -> ResourceTable:
            """Fetch a single NfsClientsCache resource

            Args:
                client_retention_interval: The lifetime range of the connected-clients cache. Only intervals in multiples of 12 hours or its equivalent in days, minutes or seconds are supported. The minimum is 12 hours and the maximum is 168 hours or 7 days. 
                enable_nfs_clients_deletion: Specifies whether or not NFS Clients deletion is enabled for the connected-clients cache. When set to \"true\", connected-clients entries are deleted when a connection is closed. 
                update_interval: The time interval between refreshing the connected-clients cache. The minimum is 1 hour and the maximum is 8 hours. 
            """

            kwargs = {}
            if client_retention_interval is not None:
                kwargs["client_retention_interval"] = client_retention_interval
            if enable_nfs_clients_deletion is not None:
                kwargs["enable_nfs_clients_deletion"] = enable_nfs_clients_deletion
            if update_interval is not None:
                kwargs["update_interval"] = update_interval
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            resource = NfsClientsCache(
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
        r"""Updates the properties of the NFS connected-client cache settings.

### Learn more
* [`DOC /protocols/nfs/connected-client-settings`](#docs-NAS-protocols_nfs_connected-client-settings)"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="nfs clients cache modify")
        async def nfs_clients_cache_modify(
        ) -> ResourceTable:
            """Modify an instance of a NfsClientsCache resource

            Args:
                client_retention_interval: The lifetime range of the connected-clients cache. Only intervals in multiples of 12 hours or its equivalent in days, minutes or seconds are supported. The minimum is 12 hours and the maximum is 168 hours or 7 days. 
                query_client_retention_interval: The lifetime range of the connected-clients cache. Only intervals in multiples of 12 hours or its equivalent in days, minutes or seconds are supported. The minimum is 12 hours and the maximum is 168 hours or 7 days. 
                enable_nfs_clients_deletion: Specifies whether or not NFS Clients deletion is enabled for the connected-clients cache. When set to \"true\", connected-clients entries are deleted when a connection is closed. 
                query_enable_nfs_clients_deletion: Specifies whether or not NFS Clients deletion is enabled for the connected-clients cache. When set to \"true\", connected-clients entries are deleted when a connection is closed. 
                update_interval: The time interval between refreshing the connected-clients cache. The minimum is 1 hour and the maximum is 8 hours. 
                query_update_interval: The time interval between refreshing the connected-clients cache. The minimum is 1 hour and the maximum is 8 hours. 
            """

            kwargs = {}
            changes = {}
            if query_client_retention_interval is not None:
                kwargs["client_retention_interval"] = query_client_retention_interval
            if query_enable_nfs_clients_deletion is not None:
                kwargs["enable_nfs_clients_deletion"] = query_enable_nfs_clients_deletion
            if query_update_interval is not None:
                kwargs["update_interval"] = query_update_interval

            if client_retention_interval is not None:
                changes["client_retention_interval"] = client_retention_interval
            if enable_nfs_clients_deletion is not None:
                changes["enable_nfs_clients_deletion"] = enable_nfs_clients_deletion
            if update_interval is not None:
                changes["update_interval"] = update_interval

            if hasattr(NfsClientsCache, "find"):
                resource = NfsClientsCache.find(
                    **kwargs
                )
            else:
                resource = NfsClientsCache()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify NfsClientsCache: %s" % err)



