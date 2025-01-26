r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API is used to retrieve and manage group-membership cache settings.
## Examples
### Retrieving group-membership cache settings
---
The following example shows how to use the cache group-membership settings GET endpoint to retrieve group-membership cache settings.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import GroupMembershipSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(GroupMembershipSettings.get_collection(fields="*")))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    GroupMembershipSettings(
        {
            "svm": {"name": "vs43", "uuid": "8a1a8730-2036-11ec-8457-005056bbcfdb"},
            "ttl": "PT1H",
            "enabled": True,
        }
    ),
    GroupMembershipSettings(
        {
            "svm": {"name": "vs34", "uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"},
            "ttl": "PT5M",
            "enabled": True,
        }
    ),
]

```
</div>
</div>

---
### Retrieving group-membership cache settings for a given SVM
---
The following example shows how to use the cache group-membership settings GET endpoint to retrieve group-membership cache settings for a given SVM.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import GroupMembershipSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = GroupMembershipSettings(
        **{"svm.uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
GroupMembershipSettings(
    {
        "svm": {"name": "vs34", "uuid": "dc458b2f-2035-11ec-bfe2-005056bb6bef"},
        "ttl": "PT1H",
        "enabled": True,
    }
)

```
</div>
</div>

---
### Updating a group-membership cache setting
---
The following example shows how to use the cache group-membership settings PATCH endpoint to update a group-membership setting.
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import UnixUserSettings

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = UnixUserSettings(**{"svm.uuid": "02c9e252-41be-11e9-81d5-00a0986138f9"})
    resource.enabled = False
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


__all__ = ["GroupMembershipSettings", "GroupMembershipSettingsSchema"]
__pdoc__ = {
    "GroupMembershipSettingsSchema.resource": False,
    "GroupMembershipSettingsSchema.opts": False,
    "GroupMembershipSettings.group_membership_settings_show": False,
    "GroupMembershipSettings.group_membership_settings_create": False,
    "GroupMembershipSettings.group_membership_settings_modify": False,
    "GroupMembershipSettings.group_membership_settings_delete": False,
}


class GroupMembershipSettingsSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupMembershipSettings object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the group_membership_settings."""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Specifies whether or not the cache is enabled."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the group_membership_settings."""

    ttl = fields.Str(
        data_key="ttl",
    )
    r""" Specifies Time to Live for list of group members, in ISO 8601 format.


Example: PT2H30M"""

    @property
    def resource(self):
        return GroupMembershipSettings

    gettable_fields = [
        "links",
        "enabled",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "ttl",
    ]
    """links,enabled,svm.links,svm.name,svm.uuid,ttl,"""

    patchable_fields = [
        "enabled",
        "svm.name",
        "svm.uuid",
        "ttl",
    ]
    """enabled,svm.name,svm.uuid,ttl,"""

    postable_fields = [
        "enabled",
        "svm.name",
        "svm.uuid",
        "ttl",
    ]
    """enabled,svm.name,svm.uuid,ttl,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in GroupMembershipSettings.get_collection(fields=field)]
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
            raise NetAppRestError("GroupMembershipSettings modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class GroupMembershipSettings(Resource):
    r""" Group membership cache setting. """

    _schema = GroupMembershipSettingsSchema
    _path = "/api/name-services/cache/group-membership/settings"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves group-membership cache settings.
### Related ONTAP commands
* `vserver services name-service cache group-membership settings show`
### Learn more
* [`DOC /name-services/cache/group-membership/settings`](#docs-name-services-name-services_cache_group-membership_settings)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="group membership settings show")
        def group_membership_settings_show(
            fields: List[Choices.define(["enabled", "ttl", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of GroupMembershipSettings resources

            Args:
                enabled: Specifies whether or not the cache is enabled. 
                ttl: Specifies Time to Live for list of group members, in ISO 8601 format. 
            """

            kwargs = {}
            if enabled is not None:
                kwargs["enabled"] = enabled
            if ttl is not None:
                kwargs["ttl"] = ttl
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return GroupMembershipSettings.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all GroupMembershipSettings resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["GroupMembershipSettings"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a group-membership cache setting.
### Important notes
  - svm.uuid field cannot be empty.
  - Returns success in case no values are provided for update.
### Related ONTAP commands
* `vserver services name-service cache group-membership settings modify`
### Learn more
* [`DOC /name-services/cache/group-membership/settings`](#docs-name-services-name-services_cache_group-membership_settings)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)



    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves group-membership cache settings.
### Related ONTAP commands
* `vserver services name-service cache group-membership settings show`
### Learn more
* [`DOC /name-services/cache/group-membership/settings`](#docs-name-services-name-services_cache_group-membership_settings)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a group-membership cache setting for a given SVM.
### Related ONTAP commands
* `vserver services name-service cache group-membership settings show`
### Learn more
* [`DOC /name-services/cache/group-membership/settings`](#docs-name-services-name-services_cache_group-membership_settings)
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
        r"""Updates a group-membership cache setting.
### Important notes
  - svm.uuid field cannot be empty.
  - Returns success in case no values are provided for update.
### Related ONTAP commands
* `vserver services name-service cache group-membership settings modify`
### Learn more
* [`DOC /name-services/cache/group-membership/settings`](#docs-name-services-name-services_cache_group-membership_settings)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="group membership settings modify")
        async def group_membership_settings_modify(
        ) -> ResourceTable:
            """Modify an instance of a GroupMembershipSettings resource

            Args:
                enabled: Specifies whether or not the cache is enabled. 
                query_enabled: Specifies whether or not the cache is enabled. 
                ttl: Specifies Time to Live for list of group members, in ISO 8601 format. 
                query_ttl: Specifies Time to Live for list of group members, in ISO 8601 format. 
            """

            kwargs = {}
            changes = {}
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_ttl is not None:
                kwargs["ttl"] = query_ttl

            if enabled is not None:
                changes["enabled"] = enabled
            if ttl is not None:
                changes["ttl"] = ttl

            if hasattr(GroupMembershipSettings, "find"):
                resource = GroupMembershipSettings.find(
                    **kwargs
                )
            else:
                resource = GroupMembershipSettings()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify GroupMembershipSettings: %s" % err)



