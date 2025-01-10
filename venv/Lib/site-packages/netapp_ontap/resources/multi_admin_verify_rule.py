r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
These APIs provide information about a specific multi-admin verification rule.
Rules define the ONTAP commands (operations) that should be protected by multi-admin approval.
While the feature is turned on, any ONTAP operation that is defined with a rule will be enforced with multi-admin approval to execute the command (operation).
<br />
---
## Examples
### Retrieving a multi-admin-verify rule
Displays information about a specific multi admin verification rule.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyRule

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyRule(
        operation="volume+delete",
        **{"owner.uuid": "52b75787-7011-11ec-a23d-005056a78fd5"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
MultiAdminVerifyRule(
    {
        "required_approvers": 1,
        "query": "-vserver vs0",
        "create_time": "2022-01-07T22:14:03-05:00",
        "operation": "volume delete",
        "system_defined": False,
        "owner": {
            "name": "cluster1",
            "_links": {
                "self": {"href": "/api/svm/svms/52b75787-7011-11ec-a23d-005056a78fd5"}
            },
            "uuid": "52b75787-7011-11ec-a23d-005056a78fd5",
        },
        "auto_request_create": True,
    }
)

```
</div>
</div>

---
### Updating a multi-admin-verify rule
Modifies the attributes of the rule.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyRule

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyRule(
        operation="volume+delete",
        **{"owner.uuid": "52b75787-7011-11ec-a23d-005056a78fd5"}
    )
    resource.required_approvers = 1
    resource.patch()

```

---
### Deleting a multi-admin-verify rule
Deletes the specified approval group.
<br />
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import MultiAdminVerifyRule

with HostConnection(
    "<cluster-ip>", username="admin", password="password", verify=False
):
    resource = MultiAdminVerifyRule(
        operation="volume+delete",
        **{"owner.uuid": "52b75787-7011-11ec-a23d-005056a78fd5"}
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


__all__ = ["MultiAdminVerifyRule", "MultiAdminVerifyRuleSchema"]
__pdoc__ = {
    "MultiAdminVerifyRuleSchema.resource": False,
    "MultiAdminVerifyRuleSchema.opts": False,
    "MultiAdminVerifyRule.multi_admin_verify_rule_show": False,
    "MultiAdminVerifyRule.multi_admin_verify_rule_create": False,
    "MultiAdminVerifyRule.multi_admin_verify_rule_modify": False,
    "MultiAdminVerifyRule.multi_admin_verify_rule_delete": False,
}


class MultiAdminVerifyRuleSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the MultiAdminVerifyRule object"""

    approval_expiry = fields.Str(
        data_key="approval_expiry",
    )
    r""" Time for requests to be approved, in ISO-8601 duration format. If not set, the global setting is used."""

    approval_groups = fields.List(fields.Nested("netapp_ontap.resources.multi_admin_verify_approval_group.MultiAdminVerifyApprovalGroupSchema", unknown=EXCLUDE), data_key="approval_groups")
    r""" List of approval groups that are allowed to approve requests for rules that don't have approval groups."""

    auto_request_create = fields.Boolean(
        data_key="auto_request_create",
    )
    r""" When true, ONTAP automatically creates a request for any failed operation where there is no matching pending request."""

    create_time = ImpreciseDateTime(
        data_key="create_time",
    )
    r""" The create_time field of the multi_admin_verify_rule."""

    execution_expiry = fields.Str(
        data_key="execution_expiry",
    )
    r""" Time for requests to be executed once approved, in ISO-8601 duration format. If not set, the global setting is used."""

    operation = fields.Str(
        data_key="operation",
    )
    r""" Command that requires one or more approvals."""

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="owner", unknown=EXCLUDE)
    r""" The owner field of the multi_admin_verify_rule."""

    query = fields.Str(
        data_key="query",
    )
    r""" When specified, this property limits the entries that require approvals to those that match the specified query."""

    required_approvers = Size(
        data_key="required_approvers",
    )
    r""" The number of required approvers, excluding the user that made the request."""

    system_defined = fields.Boolean(
        data_key="system_defined",
    )
    r""" Specifies whether the rule is system-defined or user-defined."""

    @property
    def resource(self):
        return MultiAdminVerifyRule

    gettable_fields = [
        "approval_expiry",
        "approval_groups.name",
        "auto_request_create",
        "create_time",
        "execution_expiry",
        "operation",
        "owner.links",
        "owner.name",
        "owner.uuid",
        "query",
        "required_approvers",
        "system_defined",
    ]
    """approval_expiry,approval_groups.name,auto_request_create,create_time,execution_expiry,operation,owner.links,owner.name,owner.uuid,query,required_approvers,system_defined,"""

    patchable_fields = [
        "approval_expiry",
        "approval_groups.name",
        "auto_request_create",
        "execution_expiry",
        "query",
        "required_approvers",
    ]
    """approval_expiry,approval_groups.name,auto_request_create,execution_expiry,query,required_approvers,"""

    postable_fields = [
        "approval_expiry",
        "approval_groups.name",
        "auto_request_create",
        "execution_expiry",
        "operation",
        "owner.name",
        "owner.uuid",
        "query",
        "required_approvers",
    ]
    """approval_expiry,approval_groups.name,auto_request_create,execution_expiry,operation,owner.name,owner.uuid,query,required_approvers,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in MultiAdminVerifyRule.get_collection(fields=field)]
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
            raise NetAppRestError("MultiAdminVerifyRule modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class MultiAdminVerifyRule(Resource):
    """Allows interaction with MultiAdminVerifyRule objects on the host"""

    _schema = MultiAdminVerifyRuleSchema
    _path = "/api/security/multi-admin-verify/rules"
    _keys = ["owner.uuid", "operation"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves multi-admin-verify rules.

### Learn more
* [`DOC /security/multi-admin-verify/rules`](#docs-security-security_multi-admin-verify_rules)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify rule show")
        def multi_admin_verify_rule_show(
            fields: List[Choices.define(["approval_expiry", "auto_request_create", "create_time", "execution_expiry", "operation", "query", "required_approvers", "system_defined", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of MultiAdminVerifyRule resources

            Args:
                approval_expiry: Time for requests to be approved, in ISO-8601 duration format. If not set, the global setting is used.
                auto_request_create: When true, ONTAP automatically creates a request for any failed operation where there is no matching pending request.
                create_time: 
                execution_expiry: Time for requests to be executed once approved, in ISO-8601 duration format. If not set, the global setting is used.
                operation: Command that requires one or more approvals.
                query: When specified, this property limits the entries that require approvals to those that match the specified query.
                required_approvers: The number of required approvers, excluding the user that made the request.
                system_defined: Specifies whether the rule is system-defined or user-defined.
            """

            kwargs = {}
            if approval_expiry is not None:
                kwargs["approval_expiry"] = approval_expiry
            if auto_request_create is not None:
                kwargs["auto_request_create"] = auto_request_create
            if create_time is not None:
                kwargs["create_time"] = create_time
            if execution_expiry is not None:
                kwargs["execution_expiry"] = execution_expiry
            if operation is not None:
                kwargs["operation"] = operation
            if query is not None:
                kwargs["query"] = query
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if system_defined is not None:
                kwargs["system_defined"] = system_defined
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return MultiAdminVerifyRule.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all MultiAdminVerifyRule resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["MultiAdminVerifyRule"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules/{owner.uuid}/{operation}`](#docs-security-security_multi-admin-verify_rules_{owner.uuid}_{operation})"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["MultiAdminVerifyRule"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["MultiAdminVerifyRule"], NetAppResponse]:
        r"""Creates a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules`](#docs-security-security_multi-admin-verify_rules)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["MultiAdminVerifyRule"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules/{owner.uuid}/{operation}`](#docs-security-security_multi-admin-verify_rules_{owner.uuid}_{operation})"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves multi-admin-verify rules.

### Learn more
* [`DOC /security/multi-admin-verify/rules`](#docs-security-security_multi-admin-verify_rules)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules/{owner.uuid}/{operation}`](#docs-security-security_multi-admin-verify_rules_{owner.uuid}_{operation})"""
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
        r"""Creates a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules`](#docs-security-security_multi-admin-verify_rules)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify rule create")
        async def multi_admin_verify_rule_create(
        ) -> ResourceTable:
            """Create an instance of a MultiAdminVerifyRule resource

            Args:
                approval_expiry: Time for requests to be approved, in ISO-8601 duration format. If not set, the global setting is used.
                approval_groups: List of approval groups that are allowed to approve requests for rules that don't have approval groups.
                auto_request_create: When true, ONTAP automatically creates a request for any failed operation where there is no matching pending request.
                create_time: 
                execution_expiry: Time for requests to be executed once approved, in ISO-8601 duration format. If not set, the global setting is used.
                operation: Command that requires one or more approvals.
                owner: 
                query: When specified, this property limits the entries that require approvals to those that match the specified query.
                required_approvers: The number of required approvers, excluding the user that made the request.
                system_defined: Specifies whether the rule is system-defined or user-defined.
            """

            kwargs = {}
            if approval_expiry is not None:
                kwargs["approval_expiry"] = approval_expiry
            if approval_groups is not None:
                kwargs["approval_groups"] = approval_groups
            if auto_request_create is not None:
                kwargs["auto_request_create"] = auto_request_create
            if create_time is not None:
                kwargs["create_time"] = create_time
            if execution_expiry is not None:
                kwargs["execution_expiry"] = execution_expiry
            if operation is not None:
                kwargs["operation"] = operation
            if owner is not None:
                kwargs["owner"] = owner
            if query is not None:
                kwargs["query"] = query
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if system_defined is not None:
                kwargs["system_defined"] = system_defined

            resource = MultiAdminVerifyRule(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create MultiAdminVerifyRule: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules/{owner.uuid}/{operation}`](#docs-security-security_multi-admin-verify_rules_{owner.uuid}_{operation})"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify rule modify")
        async def multi_admin_verify_rule_modify(
        ) -> ResourceTable:
            """Modify an instance of a MultiAdminVerifyRule resource

            Args:
                approval_expiry: Time for requests to be approved, in ISO-8601 duration format. If not set, the global setting is used.
                query_approval_expiry: Time for requests to be approved, in ISO-8601 duration format. If not set, the global setting is used.
                auto_request_create: When true, ONTAP automatically creates a request for any failed operation where there is no matching pending request.
                query_auto_request_create: When true, ONTAP automatically creates a request for any failed operation where there is no matching pending request.
                create_time: 
                query_create_time: 
                execution_expiry: Time for requests to be executed once approved, in ISO-8601 duration format. If not set, the global setting is used.
                query_execution_expiry: Time for requests to be executed once approved, in ISO-8601 duration format. If not set, the global setting is used.
                operation: Command that requires one or more approvals.
                query_operation: Command that requires one or more approvals.
                query: When specified, this property limits the entries that require approvals to those that match the specified query.
                query_query: When specified, this property limits the entries that require approvals to those that match the specified query.
                required_approvers: The number of required approvers, excluding the user that made the request.
                query_required_approvers: The number of required approvers, excluding the user that made the request.
                system_defined: Specifies whether the rule is system-defined or user-defined.
                query_system_defined: Specifies whether the rule is system-defined or user-defined.
            """

            kwargs = {}
            changes = {}
            if query_approval_expiry is not None:
                kwargs["approval_expiry"] = query_approval_expiry
            if query_auto_request_create is not None:
                kwargs["auto_request_create"] = query_auto_request_create
            if query_create_time is not None:
                kwargs["create_time"] = query_create_time
            if query_execution_expiry is not None:
                kwargs["execution_expiry"] = query_execution_expiry
            if query_operation is not None:
                kwargs["operation"] = query_operation
            if query_query is not None:
                kwargs["query"] = query_query
            if query_required_approvers is not None:
                kwargs["required_approvers"] = query_required_approvers
            if query_system_defined is not None:
                kwargs["system_defined"] = query_system_defined

            if approval_expiry is not None:
                changes["approval_expiry"] = approval_expiry
            if auto_request_create is not None:
                changes["auto_request_create"] = auto_request_create
            if create_time is not None:
                changes["create_time"] = create_time
            if execution_expiry is not None:
                changes["execution_expiry"] = execution_expiry
            if operation is not None:
                changes["operation"] = operation
            if query is not None:
                changes["query"] = query
            if required_approvers is not None:
                changes["required_approvers"] = required_approvers
            if system_defined is not None:
                changes["system_defined"] = system_defined

            if hasattr(MultiAdminVerifyRule, "find"):
                resource = MultiAdminVerifyRule.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyRule()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify MultiAdminVerifyRule: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes a multi-admin-verify rule.

### Learn more
* [`DOC /security/multi-admin-verify/rules/{owner.uuid}/{operation}`](#docs-security-security_multi-admin-verify_rules_{owner.uuid}_{operation})"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="multi admin verify rule delete")
        async def multi_admin_verify_rule_delete(
        ) -> None:
            """Delete an instance of a MultiAdminVerifyRule resource

            Args:
                approval_expiry: Time for requests to be approved, in ISO-8601 duration format. If not set, the global setting is used.
                auto_request_create: When true, ONTAP automatically creates a request for any failed operation where there is no matching pending request.
                create_time: 
                execution_expiry: Time for requests to be executed once approved, in ISO-8601 duration format. If not set, the global setting is used.
                operation: Command that requires one or more approvals.
                query: When specified, this property limits the entries that require approvals to those that match the specified query.
                required_approvers: The number of required approvers, excluding the user that made the request.
                system_defined: Specifies whether the rule is system-defined or user-defined.
            """

            kwargs = {}
            if approval_expiry is not None:
                kwargs["approval_expiry"] = approval_expiry
            if auto_request_create is not None:
                kwargs["auto_request_create"] = auto_request_create
            if create_time is not None:
                kwargs["create_time"] = create_time
            if execution_expiry is not None:
                kwargs["execution_expiry"] = execution_expiry
            if operation is not None:
                kwargs["operation"] = operation
            if query is not None:
                kwargs["query"] = query
            if required_approvers is not None:
                kwargs["required_approvers"] = required_approvers
            if system_defined is not None:
                kwargs["system_defined"] = system_defined

            if hasattr(MultiAdminVerifyRule, "find"):
                resource = MultiAdminVerifyRule.find(
                    **kwargs
                )
            else:
                resource = MultiAdminVerifyRule()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete MultiAdminVerifyRule: %s" % err)


