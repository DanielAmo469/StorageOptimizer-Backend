r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
These APIs provide information on the multi-admin verification global setting.
The GET API retrieves the object store that contains the global setting values of the multi-admin-verify feature.
The PATCH request is used to modify the multi-admin-verify global setting. All fields are optional for the PATCH request.
<br />
---
## Examples
### Retrieving the multi-admin-verify global setting
Retrieves the current multi-admin-verify global setting. If the global setting is not set, default values are returned.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyConfig

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyConfig()
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
MultiAdminVerifyConfig(
    {
        "required_approvers": 1,
        "execution_expiry": "PT1H",
        "approval_expiry": "PT1H",
        "approval_groups": [],
        "enabled": False,
    }
)

```
</div>
</div>

---
### Updating the multi-admin-verify global setting
The following example updates the multi-admin-verify global settings.
Note that the approval_groups needs to be available in /security/multi-admin-verify/approval-groups before it is set in the global setting.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyConfig

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyConfig()
    resource.required_approvers = "1"
    resource.enabled = True
    resource.execution_expiry = "2h"
    resource.approval_expiry = "3h"
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


__all__ = ["MultiAdminVerifyConfig", "MultiAdminVerifyConfigSchema"]
__pdoc__ = {
    "MultiAdminVerifyConfigSchema.resource": False,
    "MultiAdminVerifyConfigSchema.opts": False,
    "MultiAdminVerifyConfig.multi_admin_verify_config_show": False,
    "MultiAdminVerifyConfig.multi_admin_verify_config_create": False,
    "MultiAdminVerifyConfig.multi_admin_verify_config_modify": False,
    "MultiAdminVerifyConfig.multi_admin_verify_config_delete": False,
}


class MultiAdminVerifyConfigSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MultiAdminVerifyConfig object"""

    approval_expiry = fields.Str(
        data_key="approval_expiry",
    )
    r""" Default time for requests to be approved, in ISO-8601 duration format."""

    approval_groups = fields.List(fields.Str, data_key="approval_groups")
    r""" List of approval groups that are allowed to approve requests for rules that don't have approval groups."""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" The enabled field of the multi_admin_verify_config."""

    execution_expiry = fields.Str(
        data_key="execution_expiry",
    )
    r""" Default time for requests to be executed once approved, in ISO-8601 duration format."""

    required_approvers = Size(
        data_key="required_approvers",
    )
    r""" The number of required approvers, excluding the user that made the request."""

    @property
    def resource(self):
        return MultiAdminVerifyConfig

    gettable_fields = [
        "approval_expiry",
        "approval_groups",
        "enabled",
        "execution_expiry",
        "required_approvers",
    ]
    """approval_expiry,approval_groups,enabled,execution_expiry,required_approvers,"""

    patchable_fields = [
        "approval_expiry",
        "approval_groups",
        "enabled",
        "execution_expiry",
        "required_approvers",
    ]
    """approval_expiry,approval_groups,enabled,execution_expiry,required_approvers,"""

    postable_fields = [
        "approval_expiry",
        "approval_groups",
        "enabled",
        "execution_expiry",
        "required_approvers",
    ]
    """approval_expiry,approval_groups,enabled,execution_expiry,required_approvers,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in MultiAdminVerifyConfig.get_collection(fields=field)]
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
            raise NetAppRestError("MultiAdminVerifyConfig modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class MultiAdminVerifyConfig(Resource):
    """Allows interaction with MultiAdminVerifyConfig objects on the host"""

    _schema = MultiAdminVerifyConfigSchema
    _path = "/api/security/multi-admin-verify"






    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the multi-admin-verify configuration.

### Learn more
* [`DOC /security/multi-admin-verify`](#docs-security-security_multi-admin-verify)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify config show")
        def multi_admin_verify_config_show(
            fields: List[str] = None,
        ) -> ResourceTable:
            """Fetch a single MultiAdminVerifyConfig resource

            Args:
                approval_expiry: Default time for requests to be approved, in ISO-8601 duration format.
                approval_groups: List of approval groups that are allowed to approve requests for rules that don't have approval groups.
                enabled: 
                execution_expiry: Default time for requests to be executed once approved, in ISO-8601 duration format.
                required_approvers: The number of required approvers, excluding the user that made the request.
            """

            kwargs = {}
            if approval_expiry is not None:
                kwargs["approval_expiry"] = approval_expiry
            if approval_groups is not None:
                kwargs["approval_groups"] = approval_groups
            if enabled is not None:
                kwargs["enabled"] = enabled
            if execution_expiry is not None:
                kwargs["execution_expiry"] = execution_expiry
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            resource = MultiAdminVerifyConfig(
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
        r"""Modifies the multi-admin-verify configuration.

### Learn more
* [`DOC /security/multi-admin-verify`](#docs-security-security_multi-admin-verify)"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify config modify")
        async def multi_admin_verify_config_modify(
        ) -> ResourceTable:
            """Modify an instance of a MultiAdminVerifyConfig resource

            Args:
                approval_expiry: Default time for requests to be approved, in ISO-8601 duration format.
                query_approval_expiry: Default time for requests to be approved, in ISO-8601 duration format.
                approval_groups: List of approval groups that are allowed to approve requests for rules that don't have approval groups.
                query_approval_groups: List of approval groups that are allowed to approve requests for rules that don't have approval groups.
                enabled: 
                query_enabled: 
                execution_expiry: Default time for requests to be executed once approved, in ISO-8601 duration format.
                query_execution_expiry: Default time for requests to be executed once approved, in ISO-8601 duration format.
                required_approvers: The number of required approvers, excluding the user that made the request.
                query_required_approvers: The number of required approvers, excluding the user that made the request.
            """

            kwargs = {}
            changes = {}
            if query_approval_expiry is not None:
                kwargs["approval_expiry"] = query_approval_expiry
            if query_approval_groups is not None:
                kwargs["approval_groups"] = query_approval_groups
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_execution_expiry is not None:
                kwargs["execution_expiry"] = query_execution_expiry
            if query_required_approvers is not None:
                kwargs["required_approvers"] = query_required_approvers

            if approval_expiry is not None:
                changes["approval_expiry"] = approval_expiry
            if approval_groups is not None:
                changes["approval_groups"] = approval_groups
            if enabled is not None:
                changes["enabled"] = enabled
            if execution_expiry is not None:
                changes["execution_expiry"] = execution_expiry
            if required_approvers is not None:
                changes["required_approvers"] = required_approvers

            if hasattr(MultiAdminVerifyConfig, "find"):
                resource = MultiAdminVerifyConfig.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyConfig()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify MultiAdminVerifyConfig: %s" % err)



