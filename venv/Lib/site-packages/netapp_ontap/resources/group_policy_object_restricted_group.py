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


__all__ = ["GroupPolicyObjectRestrictedGroup", "GroupPolicyObjectRestrictedGroupSchema"]
__pdoc__ = {
    "GroupPolicyObjectRestrictedGroupSchema.resource": False,
    "GroupPolicyObjectRestrictedGroupSchema.opts": False,
    "GroupPolicyObjectRestrictedGroup.group_policy_object_restricted_group_show": False,
    "GroupPolicyObjectRestrictedGroup.group_policy_object_restricted_group_create": False,
    "GroupPolicyObjectRestrictedGroup.group_policy_object_restricted_group_modify": False,
    "GroupPolicyObjectRestrictedGroup.group_policy_object_restricted_group_delete": False,
}


class GroupPolicyObjectRestrictedGroupSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObjectRestrictedGroup object"""

    group_name = fields.Str(
        data_key="group_name",
        validate=len_validation(minimum=1),
    )
    r""" The group_name field of the group_policy_object_restricted_group.

Example: test_group"""

    link = fields.Str(
        data_key="link",
        validate=enum_validation(['local', 'site', 'domain', 'organizational_unit', 'rsop']),
    )
    r""" Link info.

Valid choices:

* local
* site
* domain
* organizational_unit
* rsop"""

    members = fields.List(fields.Str, data_key="members")
    r""" Members of the group.

Example: ["DOMAIN/test_user","DOMAIN/user2"]"""

    memberships = fields.List(fields.Str, data_key="memberships")
    r""" Group is member of Group/OU.

Example: ["DOMAIN/AdministratorGrp","DOMAIN/deptMark"]"""

    policy_name = fields.Str(
        data_key="policy_name",
        validate=len_validation(minimum=1),
    )
    r""" The policy_name field of the group_policy_object_restricted_group.

Example: test_policy"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the group_policy_object_restricted_group."""

    version = Size(
        data_key="version",
    )
    r""" Group policy object version.

Example: 7"""

    @property
    def resource(self):
        return GroupPolicyObjectRestrictedGroup

    gettable_fields = [
        "group_name",
        "link",
        "members",
        "memberships",
        "policy_name",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "version",
    ]
    """group_name,link,members,memberships,policy_name,svm.links,svm.name,svm.uuid,version,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in GroupPolicyObjectRestrictedGroup.get_collection(fields=field)]
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
            raise NetAppRestError("GroupPolicyObjectRestrictedGroup modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class GroupPolicyObjectRestrictedGroup(Resource):
    """Allows interaction with GroupPolicyObjectRestrictedGroup objects on the host"""

    _schema = GroupPolicyObjectRestrictedGroupSchema
    _path = "/api/protocols/cifs/group-policies/{svm[uuid]}/restricted-groups"
    _keys = ["svm.uuid", "policy_index", "group_name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves applied policies of restricted groups for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy restricted-group show-applied`
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="group policy object restricted group show")
        def group_policy_object_restricted_group_show(
            svm_uuid,
            group_name: Choices.define(_get_field_list("group_name"), cache_choices=True, inexact=True)=None,
            link: Choices.define(_get_field_list("link"), cache_choices=True, inexact=True)=None,
            members: Choices.define(_get_field_list("members"), cache_choices=True, inexact=True)=None,
            memberships: Choices.define(_get_field_list("memberships"), cache_choices=True, inexact=True)=None,
            policy_name: Choices.define(_get_field_list("policy_name"), cache_choices=True, inexact=True)=None,
            version: Choices.define(_get_field_list("version"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["group_name", "link", "members", "memberships", "policy_name", "version", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of GroupPolicyObjectRestrictedGroup resources

            Args:
                group_name: 
                link: Link info.
                members: Members of the group.
                memberships: Group is member of Group/OU.
                policy_name: 
                version: Group policy object version.
            """

            kwargs = {}
            if group_name is not None:
                kwargs["group_name"] = group_name
            if link is not None:
                kwargs["link"] = link
            if members is not None:
                kwargs["members"] = members
            if memberships is not None:
                kwargs["memberships"] = memberships
            if policy_name is not None:
                kwargs["policy_name"] = policy_name
            if version is not None:
                kwargs["version"] = version
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return GroupPolicyObjectRestrictedGroup.get_collection(
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
        """Returns a count of all GroupPolicyObjectRestrictedGroup resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves applied policies of restricted groups for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy restricted-group show-applied`
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves applied policy of restricted group for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy restricted-group show-applied`
"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





