r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
These APIs provide information about a specific multi-admin verification request.
If you need to execute a command that is protected by a multi-admin rule, you must first submit a request to be allowed to execute the command.
The request must then be approved by the designated approvers according to the rule associated with the command.
<br />
---
## Examples
### Retrieving a multi-admin-verify request
Retrieves information about a specific multi-admin verification request.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyRequest

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyRequest(index=1)
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
MultiAdminVerifyRequest(
    {
        "index": 1,
        "execute_on_approval": False,
        "permitted_users": ["wenbo"],
        "query": "",
        "required_approvers": 1,
        "user_requested": "admin",
        "approve_expiry_time": "2022-01-05T21:07:09-05:00",
        "create_time": "2022-01-05T20:07:09-05:00",
        "operation": "security multi-admin-verify modify",
        "pending_approvers": 1,
        "state": "expired",
        "owner": {
            "name": "cluster1",
            "_links": {
                "self": {"href": "/api/svm/svms/c1483186-6e73-11ec-bc92-005056a7ad04"}
            },
            "uuid": "c1483186-6e73-11ec-bc92-005056a7ad04",
        },
    }
)

```
</div>
</div>

---
### Updating a multi-admin-verify request
Updates a specific multi-admin-verify request
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyRequest

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyRequest(index=1)
    resource.state = "approved"
    resource.execute_on_approval = False
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


__all__ = ["MultiAdminVerifyRequest", "MultiAdminVerifyRequestSchema"]
__pdoc__ = {
    "MultiAdminVerifyRequestSchema.resource": False,
    "MultiAdminVerifyRequestSchema.opts": False,
    "MultiAdminVerifyRequest.multi_admin_verify_request_show": False,
    "MultiAdminVerifyRequest.multi_admin_verify_request_create": False,
    "MultiAdminVerifyRequest.multi_admin_verify_request_modify": False,
    "MultiAdminVerifyRequest.multi_admin_verify_request_delete": False,
}


class MultiAdminVerifyRequestSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MultiAdminVerifyRequest object"""

    approve_expiry_time = ImpreciseDateTime(
        data_key="approve_expiry_time",
    )
    r""" The approve_expiry_time field of the multi_admin_verify_request."""

    approve_time = ImpreciseDateTime(
        data_key="approve_time",
    )
    r""" The approve_time field of the multi_admin_verify_request."""

    approved_users = fields.List(fields.Str, data_key="approved_users")
    r""" The users that have approved the request."""

    comment = fields.Str(
        data_key="comment",
    )
    r""" Optional user-provided comment that is sent to the approval-group email indicating why the request was made."""

    create_time = ImpreciseDateTime(
        data_key="create_time",
    )
    r""" The create_time field of the multi_admin_verify_request."""

    execute_on_approval = fields.Boolean(
        data_key="execute_on_approval",
    )
    r""" Specifies that the operation is executed automatically on final approval."""

    execution_expiry_time = ImpreciseDateTime(
        data_key="execution_expiry_time",
    )
    r""" The execution_expiry_time field of the multi_admin_verify_request."""

    index = Size(
        data_key="index",
    )
    r""" Unique index that represents a request."""

    operation = fields.Str(
        data_key="operation",
    )
    r""" The command to execute."""

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="owner", unknown=EXCLUDE)
    r""" The owner field of the multi_admin_verify_request."""

    pending_approvers = Size(
        data_key="pending_approvers",
    )
    r""" The number of approvers remaining that are required to approve."""

    permitted_users = fields.List(fields.Str, data_key="permitted_users")
    r""" List of users that can execute the operation once approved. If not set, any authorized user can perform the operation."""

    potential_approvers = fields.List(fields.Str, data_key="potential_approvers")
    r""" The users that are able to approve the request."""

    query = fields.Str(
        data_key="query",
    )
    r""" Identifies the specific entry upon which the user wants to operate."""

    required_approvers = Size(
        data_key="required_approvers",
    )
    r""" The number of required approvers, excluding the user that made the request."""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['pending', 'approved', 'vetoed', 'expired', 'executed']),
    )
    r""" The state of the request. PATCH supports approved and vetoed. The state only changes after setting to approved once no more approvers are required.

Valid choices:

* pending
* approved
* vetoed
* expired
* executed"""

    user_requested = fields.Str(
        data_key="user_requested",
    )
    r""" The user that created the request. Automatically set by ONTAP."""

    user_vetoed = fields.Str(
        data_key="user_vetoed",
    )
    r""" The user that vetoed the request."""

    @property
    def resource(self):
        return MultiAdminVerifyRequest

    gettable_fields = [
        "approve_expiry_time",
        "approve_time",
        "approved_users",
        "comment",
        "create_time",
        "execute_on_approval",
        "execution_expiry_time",
        "index",
        "operation",
        "owner.links",
        "owner.name",
        "owner.uuid",
        "pending_approvers",
        "permitted_users",
        "potential_approvers",
        "query",
        "required_approvers",
        "state",
        "user_requested",
        "user_vetoed",
    ]
    """approve_expiry_time,approve_time,approved_users,comment,create_time,execute_on_approval,execution_expiry_time,index,operation,owner.links,owner.name,owner.uuid,pending_approvers,permitted_users,potential_approvers,query,required_approvers,state,user_requested,user_vetoed,"""

    patchable_fields = [
        "execute_on_approval",
        "owner.name",
        "owner.uuid",
        "state",
    ]
    """execute_on_approval,owner.name,owner.uuid,state,"""

    postable_fields = [
        "comment",
        "execute_on_approval",
        "operation",
        "owner.name",
        "owner.uuid",
        "permitted_users",
        "query",
        "state",
    ]
    """comment,execute_on_approval,operation,owner.name,owner.uuid,permitted_users,query,state,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in MultiAdminVerifyRequest.get_collection(fields=field)]
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
            raise NetAppRestError("MultiAdminVerifyRequest modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class MultiAdminVerifyRequest(Resource):
    """Allows interaction with MultiAdminVerifyRequest objects on the host"""

    _schema = MultiAdminVerifyRequestSchema
    _path = "/api/security/multi-admin-verify/requests"
    _keys = ["index"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves multi-admin-verify requests.

### Learn more
* [`DOC /security/multi-admin-verify/requests`](#docs-security-security_multi-admin-verify_requests)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify request show")
        def multi_admin_verify_request_show(
            fields: List[Choices.define(["approve_expiry_time", "approve_time", "approved_users", "comment", "create_time", "execute_on_approval", "execution_expiry_time", "index", "operation", "pending_approvers", "permitted_users", "potential_approvers", "query", "required_approvers", "state", "user_requested", "user_vetoed", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of MultiAdminVerifyRequest resources

            Args:
                approve_expiry_time: 
                approve_time: 
                approved_users: The users that have approved the request.
                comment: Optional user-provided comment that is sent to the approval-group email indicating why the request was made.
                create_time: 
                execute_on_approval: Specifies that the operation is executed automatically on final approval.
                execution_expiry_time: 
                index: Unique index that represents a request.
                operation: The command to execute.
                pending_approvers: The number of approvers remaining that are required to approve.
                permitted_users: List of users that can execute the operation once approved. If not set, any authorized user can perform the operation.
                potential_approvers: The users that are able to approve the request.
                query: Identifies the specific entry upon which the user wants to operate.
                required_approvers: The number of required approvers, excluding the user that made the request.
                state: The state of the request. PATCH supports approved and vetoed. The state only changes after setting to approved once no more approvers are required.
                user_requested: The user that created the request. Automatically set by ONTAP.
                user_vetoed: The user that vetoed the request.
            """

            kwargs = {}
            if approve_expiry_time is not None:
                kwargs["approve_expiry_time"] = approve_expiry_time
            if approve_time is not None:
                kwargs["approve_time"] = approve_time
            if approved_users is not None:
                kwargs["approved_users"] = approved_users
            if comment is not None:
                kwargs["comment"] = comment
            if create_time is not None:
                kwargs["create_time"] = create_time
            if execute_on_approval is not None:
                kwargs["execute_on_approval"] = execute_on_approval
            if execution_expiry_time is not None:
                kwargs["execution_expiry_time"] = execution_expiry_time
            if index is not None:
                kwargs["index"] = index
            if operation is not None:
                kwargs["operation"] = operation
            if pending_approvers is not None:
                kwargs["pending_approvers"] = pending_approvers
            if permitted_users is not None:
                kwargs["permitted_users"] = permitted_users
            if potential_approvers is not None:
                kwargs["potential_approvers"] = potential_approvers
            if query is not None:
                kwargs["query"] = query
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if state is not None:
                kwargs["state"] = state
            if user_requested is not None:
                kwargs["user_requested"] = user_requested
            if user_vetoed is not None:
                kwargs["user_vetoed"] = user_vetoed
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return MultiAdminVerifyRequest.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all MultiAdminVerifyRequest resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["MultiAdminVerifyRequest"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests/{index}`](#docs-security-security_multi-admin-verify_requests_{index})"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["MultiAdminVerifyRequest"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["MultiAdminVerifyRequest"], NetAppResponse]:
        r"""Creates a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests`](#docs-security-security_multi-admin-verify_requests)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["MultiAdminVerifyRequest"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests/{index}`](#docs-security-security_multi-admin-verify_requests_{index})"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves multi-admin-verify requests.

### Learn more
* [`DOC /security/multi-admin-verify/requests`](#docs-security-security_multi-admin-verify_requests)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests/{index}`](#docs-security-security_multi-admin-verify_requests_{index})"""
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
        r"""Creates a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests`](#docs-security-security_multi-admin-verify_requests)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify request create")
        async def multi_admin_verify_request_create(
        ) -> ResourceTable:
            """Create an instance of a MultiAdminVerifyRequest resource

            Args:
                approve_expiry_time: 
                approve_time: 
                approved_users: The users that have approved the request.
                comment: Optional user-provided comment that is sent to the approval-group email indicating why the request was made.
                create_time: 
                execute_on_approval: Specifies that the operation is executed automatically on final approval.
                execution_expiry_time: 
                index: Unique index that represents a request.
                operation: The command to execute.
                owner: 
                pending_approvers: The number of approvers remaining that are required to approve.
                permitted_users: List of users that can execute the operation once approved. If not set, any authorized user can perform the operation.
                potential_approvers: The users that are able to approve the request.
                query: Identifies the specific entry upon which the user wants to operate.
                required_approvers: The number of required approvers, excluding the user that made the request.
                state: The state of the request. PATCH supports approved and vetoed. The state only changes after setting to approved once no more approvers are required.
                user_requested: The user that created the request. Automatically set by ONTAP.
                user_vetoed: The user that vetoed the request.
            """

            kwargs = {}
            if approve_expiry_time is not None:
                kwargs["approve_expiry_time"] = approve_expiry_time
            if approve_time is not None:
                kwargs["approve_time"] = approve_time
            if approved_users is not None:
                kwargs["approved_users"] = approved_users
            if comment is not None:
                kwargs["comment"] = comment
            if create_time is not None:
                kwargs["create_time"] = create_time
            if execute_on_approval is not None:
                kwargs["execute_on_approval"] = execute_on_approval
            if execution_expiry_time is not None:
                kwargs["execution_expiry_time"] = execution_expiry_time
            if index is not None:
                kwargs["index"] = index
            if operation is not None:
                kwargs["operation"] = operation
            if owner is not None:
                kwargs["owner"] = owner
            if pending_approvers is not None:
                kwargs["pending_approvers"] = pending_approvers
            if permitted_users is not None:
                kwargs["permitted_users"] = permitted_users
            if potential_approvers is not None:
                kwargs["potential_approvers"] = potential_approvers
            if query is not None:
                kwargs["query"] = query
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if state is not None:
                kwargs["state"] = state
            if user_requested is not None:
                kwargs["user_requested"] = user_requested
            if user_vetoed is not None:
                kwargs["user_vetoed"] = user_vetoed

            resource = MultiAdminVerifyRequest(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create MultiAdminVerifyRequest: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests/{index}`](#docs-security-security_multi-admin-verify_requests_{index})"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify request modify")
        async def multi_admin_verify_request_modify(
        ) -> ResourceTable:
            """Modify an instance of a MultiAdminVerifyRequest resource

            Args:
                approve_expiry_time: 
                query_approve_expiry_time: 
                approve_time: 
                query_approve_time: 
                approved_users: The users that have approved the request.
                query_approved_users: The users that have approved the request.
                comment: Optional user-provided comment that is sent to the approval-group email indicating why the request was made.
                query_comment: Optional user-provided comment that is sent to the approval-group email indicating why the request was made.
                create_time: 
                query_create_time: 
                execute_on_approval: Specifies that the operation is executed automatically on final approval.
                query_execute_on_approval: Specifies that the operation is executed automatically on final approval.
                execution_expiry_time: 
                query_execution_expiry_time: 
                index: Unique index that represents a request.
                query_index: Unique index that represents a request.
                operation: The command to execute.
                query_operation: The command to execute.
                pending_approvers: The number of approvers remaining that are required to approve.
                query_pending_approvers: The number of approvers remaining that are required to approve.
                permitted_users: List of users that can execute the operation once approved. If not set, any authorized user can perform the operation.
                query_permitted_users: List of users that can execute the operation once approved. If not set, any authorized user can perform the operation.
                potential_approvers: The users that are able to approve the request.
                query_potential_approvers: The users that are able to approve the request.
                query: Identifies the specific entry upon which the user wants to operate.
                query_query: Identifies the specific entry upon which the user wants to operate.
                required_approvers: The number of required approvers, excluding the user that made the request.
                query_required_approvers: The number of required approvers, excluding the user that made the request.
                state: The state of the request. PATCH supports approved and vetoed. The state only changes after setting to approved once no more approvers are required.
                query_state: The state of the request. PATCH supports approved and vetoed. The state only changes after setting to approved once no more approvers are required.
                user_requested: The user that created the request. Automatically set by ONTAP.
                query_user_requested: The user that created the request. Automatically set by ONTAP.
                user_vetoed: The user that vetoed the request.
                query_user_vetoed: The user that vetoed the request.
            """

            kwargs = {}
            changes = {}
            if query_approve_expiry_time is not None:
                kwargs["approve_expiry_time"] = query_approve_expiry_time
            if query_approve_time is not None:
                kwargs["approve_time"] = query_approve_time
            if query_approved_users is not None:
                kwargs["approved_users"] = query_approved_users
            if query_comment is not None:
                kwargs["comment"] = query_comment
            if query_create_time is not None:
                kwargs["create_time"] = query_create_time
            if query_execute_on_approval is not None:
                kwargs["execute_on_approval"] = query_execute_on_approval
            if query_execution_expiry_time is not None:
                kwargs["execution_expiry_time"] = query_execution_expiry_time
            if query_index is not None:
                kwargs["index"] = query_index
            if query_operation is not None:
                kwargs["operation"] = query_operation
            if query_pending_approvers is not None:
                kwargs["pending_approvers"] = query_pending_approvers
            if query_permitted_users is not None:
                kwargs["permitted_users"] = query_permitted_users
            if query_potential_approvers is not None:
                kwargs["potential_approvers"] = query_potential_approvers
            if query_query is not None:
                kwargs["query"] = query_query
            if query_required_approvers is not None:
                kwargs["required_approvers"] = query_required_approvers
            if query_state is not None:
                kwargs["state"] = query_state
            if query_user_requested is not None:
                kwargs["user_requested"] = query_user_requested
            if query_user_vetoed is not None:
                kwargs["user_vetoed"] = query_user_vetoed

            if approve_expiry_time is not None:
                changes["approve_expiry_time"] = approve_expiry_time
            if approve_time is not None:
                changes["approve_time"] = approve_time
            if approved_users is not None:
                changes["approved_users"] = approved_users
            if comment is not None:
                changes["comment"] = comment
            if create_time is not None:
                changes["create_time"] = create_time
            if execute_on_approval is not None:
                changes["execute_on_approval"] = execute_on_approval
            if execution_expiry_time is not None:
                changes["execution_expiry_time"] = execution_expiry_time
            if index is not None:
                changes["index"] = index
            if operation is not None:
                changes["operation"] = operation
            if pending_approvers is not None:
                changes["pending_approvers"] = pending_approvers
            if permitted_users is not None:
                changes["permitted_users"] = permitted_users
            if potential_approvers is not None:
                changes["potential_approvers"] = potential_approvers
            if query is not None:
                changes["query"] = query
            if required_approvers is not None:
                changes["required_approvers"] = required_approvers
            if state is not None:
                changes["state"] = state
            if user_requested is not None:
                changes["user_requested"] = user_requested
            if user_vetoed is not None:
                changes["user_vetoed"] = user_vetoed

            if hasattr(MultiAdminVerifyRequest, "find"):
                resource = MultiAdminVerifyRequest.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyRequest()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify MultiAdminVerifyRequest: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a multi-admin-verify request.

### Learn more
* [`DOC /security/multi-admin-verify/requests/{index}`](#docs-security-security_multi-admin-verify_requests_{index})"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify request delete")
        async def multi_admin_verify_request_delete(
        ) -> None:
            """Delete an instance of a MultiAdminVerifyRequest resource

            Args:
                approve_expiry_time: 
                approve_time: 
                approved_users: The users that have approved the request.
                comment: Optional user-provided comment that is sent to the approval-group email indicating why the request was made.
                create_time: 
                execute_on_approval: Specifies that the operation is executed automatically on final approval.
                execution_expiry_time: 
                index: Unique index that represents a request.
                operation: The command to execute.
                pending_approvers: The number of approvers remaining that are required to approve.
                permitted_users: List of users that can execute the operation once approved. If not set, any authorized user can perform the operation.
                potential_approvers: The users that are able to approve the request.
                query: Identifies the specific entry upon which the user wants to operate.
                required_approvers: The number of required approvers, excluding the user that made the request.
                state: The state of the request. PATCH supports approved and vetoed. The state only changes after setting to approved once no more approvers are required.
                user_requested: The user that created the request. Automatically set by ONTAP.
                user_vetoed: The user that vetoed the request.
            """

            kwargs = {}
            if approve_expiry_time is not None:
                kwargs["approve_expiry_time"] = approve_expiry_time
            if approve_time is not None:
                kwargs["approve_time"] = approve_time
            if approved_users is not None:
                kwargs["approved_users"] = approved_users
            if comment is not None:
                kwargs["comment"] = comment
            if create_time is not None:
                kwargs["create_time"] = create_time
            if execute_on_approval is not None:
                kwargs["execute_on_approval"] = execute_on_approval
            if execution_expiry_time is not None:
                kwargs["execution_expiry_time"] = execution_expiry_time
            if index is not None:
                kwargs["index"] = index
            if operation is not None:
                kwargs["operation"] = operation
            if pending_approvers is not None:
                kwargs["pending_approvers"] = pending_approvers
            if permitted_users is not None:
                kwargs["permitted_users"] = permitted_users
            if potential_approvers is not None:
                kwargs["potential_approvers"] = potential_approvers
            if query is not None:
                kwargs["query"] = query
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if state is not None:
                kwargs["state"] = state
            if user_requested is not None:
                kwargs["user_requested"] = user_requested
            if user_vetoed is not None:
                kwargs["user_vetoed"] = user_vetoed

            if hasattr(MultiAdminVerifyRequest, "find"):
                resource = MultiAdminVerifyRequest.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyRequest()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete MultiAdminVerifyRequest: %s" % err)


