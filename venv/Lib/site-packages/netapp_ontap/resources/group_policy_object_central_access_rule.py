r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

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


__all__ = ["GroupPolicyObjectCentralAccessRule", "GroupPolicyObjectCentralAccessRuleSchema"]
__pdoc__ = {
    "GroupPolicyObjectCentralAccessRuleSchema.resource": False,
    "GroupPolicyObjectCentralAccessRuleSchema.opts": False,
    "GroupPolicyObjectCentralAccessRule.group_policy_object_central_access_rule_show": False,
    "GroupPolicyObjectCentralAccessRule.group_policy_object_central_access_rule_create": False,
    "GroupPolicyObjectCentralAccessRule.group_policy_object_central_access_rule_modify": False,
    "GroupPolicyObjectCentralAccessRule.group_policy_object_central_access_rule_delete": False,
}


class GroupPolicyObjectCentralAccessRuleSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectCentralAccessRule object"""

    create_time = ImpreciseDateTime(
        data_key="create_time",
    )
    r""" Policy creation timestamp.

Example: 2018-01-01T16:00:00.000+0000"""

    current_permission = fields.Str(
        data_key="current_permission",
    )
    r""" Effective security policy in security descriptor definition language format.

Example: O:SYG:SYD:AR(A;;FA;;;WD)"""

    description = fields.Str(
        data_key="description",
    )
    r""" Description about the policy.

Example: rule #1"""

    name = fields.Str(
        data_key="name",
        validate=len_validation(minimum=1),
    )
    r""" The name field of the group_policy_object_central_access_rule.

Example: p1"""

    proposed_permission = fields.Str(
        data_key="proposed_permission",
    )
    r""" Proposed security policy in security descriptor definition language format.

Example: O:SYG:SYD:(A;;FA;;;OW)(A;;FA;;;BA)(A;;FA;;;SY)"""

    resource_criteria = fields.Str(
        data_key="resource_criteria",
    )
    r""" Criteria to scope resources for which access rules apply.

Example: department"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the group_policy_object_central_access_rule."""

    update_time = ImpreciseDateTime(
        data_key="update_time",
    )
    r""" Last policy modification timestamp.

Example: 2018-01-01T16:00:00.000+0000"""

    @property
    def resource(self):
        return GroupPolicyObjectCentralAccessRule

    gettable_fields = [
        "create_time",
        "current_permission",
        "description",
        "name",
        "proposed_permission",
        "resource_criteria",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "update_time",
    ]
    """create_time,current_permission,description,name,proposed_permission,resource_criteria,svm.links,svm.name,svm.uuid,update_time,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in GroupPolicyObjectCentralAccessRule.get_collection(fields=field)]
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
            raise NetAppRestError("GroupPolicyObjectCentralAccessRule modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class GroupPolicyObjectCentralAccessRule(Resource):
    """Allows interaction with GroupPolicyObjectCentralAccessRule objects on the host"""

    _schema = GroupPolicyObjectCentralAccessRuleSchema
    _path = "/api/protocols/cifs/group-policies/{svm[uuid]}/central-access-rules"
    _keys = ["svm.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves applied central access rules for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy central-access-rule show-applied`
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="group policy object central access rule show")
        def group_policy_object_central_access_rule_show(
            svm_uuid,
            create_time: Choices.define(_get_field_list("create_time"), cache_choices=True, inexact=True)=None,
            current_permission: Choices.define(_get_field_list("current_permission"), cache_choices=True, inexact=True)=None,
            description: Choices.define(_get_field_list("description"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            proposed_permission: Choices.define(_get_field_list("proposed_permission"), cache_choices=True, inexact=True)=None,
            resource_criteria: Choices.define(_get_field_list("resource_criteria"), cache_choices=True, inexact=True)=None,
            update_time: Choices.define(_get_field_list("update_time"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["create_time", "current_permission", "description", "name", "proposed_permission", "resource_criteria", "update_time", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of GroupPolicyObjectCentralAccessRule resources

            Args:
                create_time: Policy creation timestamp.
                current_permission: Effective security policy in security descriptor definition language format.
                description: Description about the policy.
                name: 
                proposed_permission: Proposed security policy in security descriptor definition language format.
                resource_criteria: Criteria to scope resources for which access rules apply.
                update_time: Last policy modification timestamp.
            """

            kwargs = {}
            if create_time is not None:
                kwargs["create_time"] = create_time
            if current_permission is not None:
                kwargs["current_permission"] = current_permission
            if description is not None:
                kwargs["description"] = description
            if name is not None:
                kwargs["name"] = name
            if proposed_permission is not None:
                kwargs["proposed_permission"] = proposed_permission
            if resource_criteria is not None:
                kwargs["resource_criteria"] = resource_criteria
            if update_time is not None:
                kwargs["update_time"] = update_time
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return GroupPolicyObjectCentralAccessRule.get_collection(
                svm_uuid,
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all GroupPolicyObjectCentralAccessRule resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves applied central access rules for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy central-access-rule show-applied`
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves applied central access rule for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy central-access-rule show-applied`
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





