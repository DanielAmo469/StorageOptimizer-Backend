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


__all__ = ["GroupPolicyObjectCentralAccessPolicy", "GroupPolicyObjectCentralAccessPolicySchema"]
__pdoc__ = {
    "GroupPolicyObjectCentralAccessPolicySchema.resource": False,
    "GroupPolicyObjectCentralAccessPolicySchema.opts": False,
    "GroupPolicyObjectCentralAccessPolicy.group_policy_object_central_access_policy_show": False,
    "GroupPolicyObjectCentralAccessPolicy.group_policy_object_central_access_policy_create": False,
    "GroupPolicyObjectCentralAccessPolicy.group_policy_object_central_access_policy_modify": False,
    "GroupPolicyObjectCentralAccessPolicy.group_policy_object_central_access_policy_delete": False,
}


class GroupPolicyObjectCentralAccessPolicySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectCentralAccessPolicy object"""

    create_time = ImpreciseDateTime(
        data_key="create_time",
    )
    r""" Policy creation timestamp.

Example: 2018-01-01T16:00:00.000+0000"""

    description = fields.Str(
        data_key="description",
    )
    r""" Description about the policy.

Example: policy #1"""

    member_rules = fields.List(fields.Str, data_key="member_rules")
    r""" Names of all central access rules applied to members.

Example: ["r1","r2"]"""

    name = fields.Str(
        data_key="name",
        validate=len_validation(minimum=1),
    )
    r""" The name field of the group_policy_object_central_access_policy.

Example: p1"""

    sid = fields.Str(
        data_key="sid",
    )
    r""" Security ID, unique identifier of the central policy.

Example: S-1-5-21-256008430-3394229847-3930036330-1001"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the group_policy_object_central_access_policy."""

    update_time = ImpreciseDateTime(
        data_key="update_time",
    )
    r""" Last policy modification timestamp.

Example: 2018-01-01T16:00:00.000+0000"""

    @property
    def resource(self):
        return GroupPolicyObjectCentralAccessPolicy

    gettable_fields = [
        "create_time",
        "description",
        "member_rules",
        "name",
        "sid",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "update_time",
    ]
    """create_time,description,member_rules,name,sid,svm.links,svm.name,svm.uuid,update_time,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in GroupPolicyObjectCentralAccessPolicy.get_collection(fields=field)]
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
            raise NetAppRestError("GroupPolicyObjectCentralAccessPolicy modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class GroupPolicyObjectCentralAccessPolicy(Resource):
    """Allows interaction with GroupPolicyObjectCentralAccessPolicy objects on the host"""

    _schema = GroupPolicyObjectCentralAccessPolicySchema
    _path = "/api/protocols/cifs/group-policies/{svm[uuid]}/central-access-policies"
    _keys = ["svm.uuid", "name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves applied central access policies for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy central-access-policy show-applied`
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="group policy object central access policy show")
        def group_policy_object_central_access_policy_show(
            svm_uuid,
            create_time: Choices.define(_get_field_list("create_time"), cache_choices=True, inexact=True)=None,
            description: Choices.define(_get_field_list("description"), cache_choices=True, inexact=True)=None,
            member_rules: Choices.define(_get_field_list("member_rules"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            sid: Choices.define(_get_field_list("sid"), cache_choices=True, inexact=True)=None,
            update_time: Choices.define(_get_field_list("update_time"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["create_time", "description", "member_rules", "name", "sid", "update_time", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of GroupPolicyObjectCentralAccessPolicy resources

            Args:
                create_time: Policy creation timestamp.
                description: Description about the policy.
                member_rules: Names of all central access rules applied to members.
                name: 
                sid: Security ID, unique identifier of the central policy.
                update_time: Last policy modification timestamp.
            """

            kwargs = {}
            if create_time is not None:
                kwargs["create_time"] = create_time
            if description is not None:
                kwargs["description"] = description
            if member_rules is not None:
                kwargs["member_rules"] = member_rules
            if name is not None:
                kwargs["name"] = name
            if sid is not None:
                kwargs["sid"] = sid
            if update_time is not None:
                kwargs["update_time"] = update_time
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return GroupPolicyObjectCentralAccessPolicy.get_collection(
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
        """Returns a count of all GroupPolicyObjectCentralAccessPolicy resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves applied central access policies for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy central-access-policy show-applied`
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves applied central access policy for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy central-access-policy show-applied`
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





