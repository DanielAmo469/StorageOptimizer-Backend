r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
When group policy objects (GPO) are enabled on your SMB server, ONTAP sends LDAP queries to the Active Directory server requesting GPO information.
If there are GPO definitions that are applicable to your SMB server, the Active Directory server returns the following GPO information:
• GPO name
• Current GPO version
• Location of the GPO definition
• Lists of UUIDs (universally unique identifiers) for GPO policy sets"""

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


__all__ = ["GroupPolicyObject", "GroupPolicyObjectSchema"]
__pdoc__ = {
    "GroupPolicyObjectSchema.resource": False,
    "GroupPolicyObjectSchema.opts": False,
    "GroupPolicyObject.group_policy_object_show": False,
    "GroupPolicyObject.group_policy_object_create": False,
    "GroupPolicyObject.group_policy_object_modify": False,
    "GroupPolicyObject.group_policy_object_delete": False,
}


class GroupPolicyObjectSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the GroupPolicyObject object"""

    central_access_policy_settings = fields.List(fields.Str, data_key="central_access_policy_settings")
    r""" Will not be populated for objects that are yet to be applied."""

    central_access_policy_staging_audit_type = fields.Str(
        data_key="central_access_policy_staging_audit_type",
        validate=enum_validation(['none', 'success', 'failure', 'both']),
    )
    r""" Types of events to be audited.

Valid choices:

* none
* success
* failure
* both"""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Specifies whether group policies are enabled for the SVM."""

    extensions = fields.List(fields.Str, data_key="extensions")
    r""" List of extensions."""

    file_system_path = fields.Str(
        data_key="file_system_path",
    )
    r""" File system path.

Example: \test.com\SysVol\test.com\policies\{42474212-3f9d-4489-ae01-6fcf4f805d4c}"""

    index = Size(
        data_key="index",
    )
    r""" Group policy object index.

Example: 1"""

    ldap_path = fields.Str(
        data_key="ldap_path",
    )
    r""" LDAP path to the GPO.

Example: cn={42474212-3f9d-4489-ae01-6fcf4f805d4c},cn=policies,cn=system,DC=TEST,DC=COM"""

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

    name = fields.Str(
        data_key="name",
        validate=len_validation(minimum=1),
    )
    r""" The name field of the group_policy_object.

Example: test_policy"""

    registry_settings = fields.Nested("netapp_ontap.models.group_policy_object_registry_setting.GroupPolicyObjectRegistrySettingSchema", data_key="registry_settings", unknown=EXCLUDE)
    r""" The registry_settings field of the group_policy_object."""

    security_settings = fields.Nested("netapp_ontap.models.group_policy_object_security_setting.GroupPolicyObjectSecuritySettingSchema", data_key="security_settings", unknown=EXCLUDE)
    r""" The security_settings field of the group_policy_object."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the group_policy_object."""

    uuid = fields.Str(
        data_key="uuid",
    )
    r""" Policy UUID.

Example: 42474212-3f9d-4489-ae01-6fcf4f805d4c"""

    version = Size(
        data_key="version",
    )
    r""" Group policy object version.

Example: 7"""

    @property
    def resource(self):
        return GroupPolicyObject

    gettable_fields = [
        "central_access_policy_settings",
        "central_access_policy_staging_audit_type",
        "enabled",
        "extensions",
        "file_system_path",
        "index",
        "ldap_path",
        "link",
        "name",
        "registry_settings",
        "security_settings",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "uuid",
        "version",
    ]
    """central_access_policy_settings,central_access_policy_staging_audit_type,enabled,extensions,file_system_path,index,ldap_path,link,name,registry_settings,security_settings,svm.links,svm.name,svm.uuid,uuid,version,"""

    patchable_fields = [
    ]
    """"""

    postable_fields = [
    ]
    """"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in GroupPolicyObject.get_collection(fields=field)]
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
            raise NetAppRestError("GroupPolicyObject modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class GroupPolicyObject(Resource):
    """Allows interaction with GroupPolicyObject objects on the host"""

    _schema = GroupPolicyObjectSchema
    _path = "/api/protocols/cifs/group-policies/{svm[uuid]}/objects"
    _keys = ["svm.uuid", "index"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves applied group policy objects for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy show-applied`

### Learn more
* [`DOC /protocols/cifs/group-policies/{svm.uuid}/objects`](#docs-NAS-protocols_cifs_group-policies_{svm.uuid}_objects)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="group policy object show")
        def group_policy_object_show(
            svm_uuid,
            central_access_policy_settings: Choices.define(_get_field_list("central_access_policy_settings"), cache_choices=True, inexact=True)=None,
            central_access_policy_staging_audit_type: Choices.define(_get_field_list("central_access_policy_staging_audit_type"), cache_choices=True, inexact=True)=None,
            enabled: Choices.define(_get_field_list("enabled"), cache_choices=True, inexact=True)=None,
            extensions: Choices.define(_get_field_list("extensions"), cache_choices=True, inexact=True)=None,
            file_system_path: Choices.define(_get_field_list("file_system_path"), cache_choices=True, inexact=True)=None,
            index: Choices.define(_get_field_list("index"), cache_choices=True, inexact=True)=None,
            ldap_path: Choices.define(_get_field_list("ldap_path"), cache_choices=True, inexact=True)=None,
            link: Choices.define(_get_field_list("link"), cache_choices=True, inexact=True)=None,
            name: Choices.define(_get_field_list("name"), cache_choices=True, inexact=True)=None,
            uuid: Choices.define(_get_field_list("uuid"), cache_choices=True, inexact=True)=None,
            version: Choices.define(_get_field_list("version"), cache_choices=True, inexact=True)=None,
            fields: List[Choices.define(["central_access_policy_settings", "central_access_policy_staging_audit_type", "enabled", "extensions", "file_system_path", "index", "ldap_path", "link", "name", "uuid", "version", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of GroupPolicyObject resources

            Args:
                central_access_policy_settings: Will not be populated for objects that are yet to be applied.
                central_access_policy_staging_audit_type: Types of events to be audited.
                enabled: Specifies whether group policies are enabled for the SVM.
                extensions: List of extensions.
                file_system_path: File system path.
                index: Group policy object index.
                ldap_path: LDAP path to the GPO.
                link: Link info.
                name: 
                uuid: Policy UUID.
                version: Group policy object version.
            """

            kwargs = {}
            if central_access_policy_settings is not None:
                kwargs["central_access_policy_settings"] = central_access_policy_settings
            if central_access_policy_staging_audit_type is not None:
                kwargs["central_access_policy_staging_audit_type"] = central_access_policy_staging_audit_type
            if enabled is not None:
                kwargs["enabled"] = enabled
            if extensions is not None:
                kwargs["extensions"] = extensions
            if file_system_path is not None:
                kwargs["file_system_path"] = file_system_path
            if index is not None:
                kwargs["index"] = index
            if ldap_path is not None:
                kwargs["ldap_path"] = ldap_path
            if link is not None:
                kwargs["link"] = link
            if name is not None:
                kwargs["name"] = name
            if uuid is not None:
                kwargs["uuid"] = uuid
            if version is not None:
                kwargs["version"] = version
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return GroupPolicyObject.get_collection(
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
        """Returns a count of all GroupPolicyObject resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves applied group policy objects for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy show-applied`

### Learn more
* [`DOC /protocols/cifs/group-policies/{svm.uuid}/objects`](#docs-NAS-protocols_cifs_group-policies_{svm.uuid}_objects)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves applied group policy object for specified SVM.
### Related ONTAP commands
* `vserver cifs group-policy show-applied`

### Learn more
* [`DOC /protocols/cifs/group-policies/{svm.uuid}/objects`](#docs-NAS-protocols_cifs_group-policies_{svm.uuid}_objects)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





