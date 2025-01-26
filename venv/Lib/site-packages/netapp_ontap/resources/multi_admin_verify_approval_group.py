r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
These APIs provide information about a specific multi-admin verification approval-group.
A group of users can be defined in a cluster server context.
Approval groups can be associated with a rule or global setting from which the associated request can retrieve approvals.
<br />
---
## Examples
### Retrieving a multi-admin-verify approval group
Displays information about a specific approval group and the users that are registered within that group.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyApprovalGroup

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyApprovalGroup(
        name="group1", **{"owner.uuid": "52b75787-7011-11ec-a23d-005056a78fd5"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
MultiAdminVerifyApprovalGroup(
    {
        "owner": {
            "name": "cluster1",
            "_links": {
                "self": {"href": "/api/svm/svms/52b75787-7011-11ec-a23d-005056a78fd5"}
            },
            "uuid": "52b75787-7011-11ec-a23d-005056a78fd5",
        },
        "name": "group1",
        "email": ["group1.approvers@email.com"],
        "approvers": ["admin"],
    }
)

```
</div>
</div>

---
### Updating a multi-admin-verify approval group
Modifies attributes of an approval group.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyApprovalGroup

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyApprovalGroup(
        name="group1", **{"owner.uuid": "52b75787-7011-11ec-a23d-005056a78fd5"}
    )
    resource.approvers = ["admin1"]
    resource.email = ["group1.approvers.new@email.com"]
    resource.patch()

```

---
### Deleting a multi-admin-verify approval group
Deletes the specified approval group.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyApprovalGroup

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyApprovalGroup(
        name="group1", **{"owner.uuid": "52b75787-7011-11ec-a23d-005056a78fd5"}
    )
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


__all__ = ["MultiAdminVerifyApprovalGroup", "MultiAdminVerifyApprovalGroupSchema"]
__pdoc__ = {
    "MultiAdminVerifyApprovalGroupSchema.resource": False,
    "MultiAdminVerifyApprovalGroupSchema.opts": False,
    "MultiAdminVerifyApprovalGroup.multi_admin_verify_approval_group_show": False,
    "MultiAdminVerifyApprovalGroup.multi_admin_verify_approval_group_create": False,
    "MultiAdminVerifyApprovalGroup.multi_admin_verify_approval_group_modify": False,
    "MultiAdminVerifyApprovalGroup.multi_admin_verify_approval_group_delete": False,
}


class MultiAdminVerifyApprovalGroupSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MultiAdminVerifyApprovalGroup object"""

    approvers = fields.List(fields.Str, data_key="approvers")
    r""" List of users that can approve a request."""

    email = fields.List(fields.Str, data_key="email")
    r""" Email addresses that are notified when a request is created, approved, vetoed, or executed."""

    name = fields.Str(
        data_key="name",
    )
    r""" Name of the approval group."""

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="owner", unknown=EXCLUDE)
    r""" The owner field of the multi_admin_verify_approval_group."""

    @property
    def resource(self):
        return MultiAdminVerifyApprovalGroup

    gettable_fields = [
        "approvers",
        "email",
        "name",
        "owner.links",
        "owner.name",
        "owner.uuid",
    ]
    """approvers,email,name,owner.links,owner.name,owner.uuid,"""

    patchable_fields = [
        "approvers",
        "email",
    ]
    """approvers,email,"""

    postable_fields = [
        "approvers",
        "email",
        "name",
        "owner.name",
        "owner.uuid",
    ]
    """approvers,email,name,owner.name,owner.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in MultiAdminVerifyApprovalGroup.get_collection(fields=field)]
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
            raise NetAppRestError("MultiAdminVerifyApprovalGroup modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class MultiAdminVerifyApprovalGroup(Resource):
    """Allows interaction with MultiAdminVerifyApprovalGroup objects on the host"""

    _schema = MultiAdminVerifyApprovalGroupSchema
    _path = "/api/security/multi-admin-verify/approval-groups"
    _keys = ["owner.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves multi-admin-verify approval groups.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups`](#docs-security-security_multi-admin-verify_approval-groups)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify approval group show")
        def multi_admin_verify_approval_group_show(
            fields: List[Choices.define(["approvers", "email", "name", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of MultiAdminVerifyApprovalGroup resources

            Args:
                approvers: List of users that can approve a request.
                email: Email addresses that are notified when a request is created, approved, vetoed, or executed.
                name: Name of the approval group.
            """

            kwargs = {}
            if approvers is not None:
                kwargs["approvers"] = approvers
            if email is not None:
                kwargs["email"] = email
            if name is not None:
                kwargs["name"] = name
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return MultiAdminVerifyApprovalGroup.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all MultiAdminVerifyApprovalGroup resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["MultiAdminVerifyApprovalGroup"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups/{owner.uuid}/{name}`](#docs-security-security_multi-admin-verify_approval-groups_{owner.uuid}_{name})"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["MultiAdminVerifyApprovalGroup"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["MultiAdminVerifyApprovalGroup"], NetAppResponse]:
        r"""Creates a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups`](#docs-security-security_multi-admin-verify_approval-groups)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["MultiAdminVerifyApprovalGroup"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups/{owner.uuid}/{name}`](#docs-security-security_multi-admin-verify_approval-groups_{owner.uuid}_{name})"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves multi-admin-verify approval groups.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups`](#docs-security-security_multi-admin-verify_approval-groups)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups/{owner.uuid}/{name}`](#docs-security-security_multi-admin-verify_approval-groups_{owner.uuid}_{name})"""
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
        r"""Creates a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups`](#docs-security-security_multi-admin-verify_approval-groups)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify approval group create")
        async def multi_admin_verify_approval_group_create(
        ) -> ResourceTable:
            """Create an instance of a MultiAdminVerifyApprovalGroup resource

            Args:
                approvers: List of users that can approve a request.
                email: Email addresses that are notified when a request is created, approved, vetoed, or executed.
                name: Name of the approval group.
                owner: 
            """

            kwargs = {}
            if approvers is not None:
                kwargs["approvers"] = approvers
            if email is not None:
                kwargs["email"] = email
            if name is not None:
                kwargs["name"] = name
            if owner is not None:
                kwargs["owner"] = owner

            resource = MultiAdminVerifyApprovalGroup(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create MultiAdminVerifyApprovalGroup: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups/{owner.uuid}/{name}`](#docs-security-security_multi-admin-verify_approval-groups_{owner.uuid}_{name})"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify approval group modify")
        async def multi_admin_verify_approval_group_modify(
        ) -> ResourceTable:
            """Modify an instance of a MultiAdminVerifyApprovalGroup resource

            Args:
                approvers: List of users that can approve a request.
                query_approvers: List of users that can approve a request.
                email: Email addresses that are notified when a request is created, approved, vetoed, or executed.
                query_email: Email addresses that are notified when a request is created, approved, vetoed, or executed.
                name: Name of the approval group.
                query_name: Name of the approval group.
            """

            kwargs = {}
            changes = {}
            if query_approvers is not None:
                kwargs["approvers"] = query_approvers
            if query_email is not None:
                kwargs["email"] = query_email
            if query_name is not None:
                kwargs["name"] = query_name

            if approvers is not None:
                changes["approvers"] = approvers
            if email is not None:
                changes["email"] = email
            if name is not None:
                changes["name"] = name

            if hasattr(MultiAdminVerifyApprovalGroup, "find"):
                resource = MultiAdminVerifyApprovalGroup.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyApprovalGroup()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify MultiAdminVerifyApprovalGroup: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a multi-admin-verify approval group.

### Learn more
* [`DOC /security/multi-admin-verify/approval-groups/{owner.uuid}/{name}`](#docs-security-security_multi-admin-verify_approval-groups_{owner.uuid}_{name})"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify approval group delete")
        async def multi_admin_verify_approval_group_delete(
        ) -> None:
            """Delete an instance of a MultiAdminVerifyApprovalGroup resource

            Args:
                approvers: List of users that can approve a request.
                email: Email addresses that are notified when a request is created, approved, vetoed, or executed.
                name: Name of the approval group.
            """

            kwargs = {}
            if approvers is not None:
                kwargs["approvers"] = approvers
            if email is not None:
                kwargs["email"] = email
            if name is not None:
                kwargs["name"] = name

            if hasattr(MultiAdminVerifyApprovalGroup, "find"):
                resource = MultiAdminVerifyApprovalGroup.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyApprovalGroup()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete MultiAdminVerifyApprovalGroup: %s" % err)


