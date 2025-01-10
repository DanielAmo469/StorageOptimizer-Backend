r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
You can use this API to display or modify Active Directory account details of the specified SVM.
It can also be used to delete an Active Directory account for the specified SVM.
## Examples
### Retrieving all Active Directory account details of a specific SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ActiveDirectory

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = ActiveDirectory(**{"svm.uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"})
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
ActiveDirectory(
    {
        "fqdn": "EXAMPLE.COM",
        "svm": {"name": "vs1", "uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"},
        "organizational_unit": "CN=Computers",
        "name": "ACCOUNT1",
    }
)

```
</div>
</div>

---
### Update the Active Directory account details of a specific SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ActiveDirectory

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = ActiveDirectory(**{"svm.uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"})
    resource.patch()

```

---
### Delete an Active Directory account of a specific SVM
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import ActiveDirectory

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = ActiveDirectory(**{"svm.uuid": "6dd78167-c907-11eb-b2bf-0050568e7324"})
    resource.delete(body={"password": "password", "username": "administrator"})

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


__all__ = ["ActiveDirectory", "ActiveDirectorySchema"]
__pdoc__ = {
    "ActiveDirectorySchema.resource": False,
    "ActiveDirectorySchema.opts": False,
    "ActiveDirectory.active_directory_show": False,
    "ActiveDirectory.active_directory_create": False,
    "ActiveDirectory.active_directory_modify": False,
    "ActiveDirectory.active_directory_delete": False,
}


class ActiveDirectorySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the ActiveDirectory object"""

    discovered_servers = fields.List(fields.Nested("netapp_ontap.models.active_directory_discovered_server.ActiveDirectoryDiscoveredServerSchema", unknown=EXCLUDE), data_key="discovered_servers")
    r""" Specifies the discovered servers records."""

    force_account_overwrite = fields.Boolean(
        data_key="force_account_overwrite",
    )
    r""" If set to true and a machine account exists with the same name as specified in "name" in Active Directory, it will be overwritten and reused.

Example: false"""

    fqdn = fields.Str(
        data_key="fqdn",
        validate=len_validation(minimum=0, maximum=254),
    )
    r""" Fully qualified domain name.

Example: server1.com"""

    name = fields.Str(
        data_key="name",
        validate=len_validation(minimum=0, maximum=15),
    )
    r""" Active Directory (AD) account NetBIOS name.

Example: account1"""

    organizational_unit = fields.Str(
        data_key="organizational_unit",
    )
    r""" Organizational unit under which the Active Directory account will be created.

Example: CN=Test"""

    password = fields.Str(
        data_key="password",
        validate=len_validation(minimum=1),
    )
    r""" Administrator password required for Active Directory account creation, modification and deletion.

Example: testpwd"""

    preferred_dcs = fields.List(fields.Nested("netapp_ontap.resources.active_directory_preferred_dc.ActiveDirectoryPreferredDcSchema", unknown=EXCLUDE), data_key="preferred_dcs")
    r""" Specifies the preferred domain controller (DC) records."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the active_directory."""

    username = fields.Str(
        data_key="username",
        validate=len_validation(minimum=1),
    )
    r""" Administrator username required for Active Directory account creation, modification and deletion.

Example: admin"""

    @property
    def resource(self):
        return ActiveDirectory

    gettable_fields = [
        "discovered_servers",
        "fqdn",
        "name",
        "organizational_unit",
        "preferred_dcs",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """discovered_servers,fqdn,name,organizational_unit,preferred_dcs,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "force_account_overwrite",
        "fqdn",
        "password",
        "username",
    ]
    """force_account_overwrite,fqdn,password,username,"""

    postable_fields = [
        "force_account_overwrite",
        "fqdn",
        "name",
        "organizational_unit",
        "password",
        "svm.name",
        "svm.uuid",
        "username",
    ]
    """force_account_overwrite,fqdn,name,organizational_unit,password,svm.name,svm.uuid,username,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in ActiveDirectory.get_collection(fields=field)]
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
            raise NetAppRestError("ActiveDirectory modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class ActiveDirectory(Resource):
    """Allows interaction with ActiveDirectory objects on the host"""

    _schema = ActiveDirectorySchema
    _path = "/api/protocols/active-directory"
    _keys = ["svm.uuid"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves Active Directory accounts for all SVMs.
### Related ONTAP commands
* `vserver active-directory show`
* `vserver active-directory preferred-dc show`
* `vserver active-directory discovered-servers show`

### Learn more
* [`DOC /protocols/active-directory`](#docs-NAS-protocols_active-directory)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="active directory show")
        def active_directory_show(
            fields: List[Choices.define(["force_account_overwrite", "fqdn", "name", "organizational_unit", "password", "username", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of ActiveDirectory resources

            Args:
                force_account_overwrite: If set to true and a machine account exists with the same name as specified in \"name\" in Active Directory, it will be overwritten and reused.
                fqdn: Fully qualified domain name.
                name: Active Directory (AD) account NetBIOS name.
                organizational_unit: Organizational unit under which the Active Directory account will be created.
                password: Administrator password required for Active Directory account creation, modification and deletion.
                username: Administrator username required for Active Directory account creation, modification and deletion.
            """

            kwargs = {}
            if force_account_overwrite is not None:
                kwargs["force_account_overwrite"] = force_account_overwrite
            if fqdn is not None:
                kwargs["fqdn"] = fqdn
            if name is not None:
                kwargs["name"] = name
            if organizational_unit is not None:
                kwargs["organizational_unit"] = organizational_unit
            if password is not None:
                kwargs["password"] = password
            if username is not None:
                kwargs["username"] = username
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return ActiveDirectory.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all ActiveDirectory resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["ActiveDirectory"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Modifies the Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory modify`
### Important notes
* Patching Active Directory account is asynchronous. Response contains Task UUID and Link that can be queried to get the status.

### Learn more
* [`DOC /protocols/active-directory/{svm.uuid}`](#docs-NAS-protocols_active-directory_{svm.uuid})"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["ActiveDirectory"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["ActiveDirectory"], NetAppResponse]:
        r"""Creates an Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory create`
### Important notes
* Active Directory account creation is asynchronous. Response contains Task UUID and Link that can be queried to get the status.

### Learn more
* [`DOC /protocols/active-directory`](#docs-NAS-protocols_active-directory)"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["ActiveDirectory"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory delete`
### Important notes
* Active Directory account deletion is asynchronous. Response contains Task UUID and Link that can be queried to get the status.

### Learn more
* [`DOC /protocols/active-directory/{svm.uuid}`](#docs-NAS-protocols_active-directory_{svm.uuid})"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves Active Directory accounts for all SVMs.
### Related ONTAP commands
* `vserver active-directory show`
* `vserver active-directory preferred-dc show`
* `vserver active-directory discovered-servers show`

### Learn more
* [`DOC /protocols/active-directory`](#docs-NAS-protocols_active-directory)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory show`
* `vserver active-directory preferred-dc show`
* `vserver active-directory discovered-servers show`
* `vserver active-directory discovered-servers reset-servers`

### Learn more
* [`DOC /protocols/active-directory/{svm.uuid}`](#docs-NAS-protocols_active-directory_{svm.uuid})"""
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
        r"""Creates an Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory create`
### Important notes
* Active Directory account creation is asynchronous. Response contains Task UUID and Link that can be queried to get the status.

### Learn more
* [`DOC /protocols/active-directory`](#docs-NAS-protocols_active-directory)"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="active directory create")
        async def active_directory_create(
        ) -> ResourceTable:
            """Create an instance of a ActiveDirectory resource

            Args:
                discovered_servers: Specifies the discovered servers records.
                force_account_overwrite: If set to true and a machine account exists with the same name as specified in \"name\" in Active Directory, it will be overwritten and reused.
                fqdn: Fully qualified domain name.
                name: Active Directory (AD) account NetBIOS name.
                organizational_unit: Organizational unit under which the Active Directory account will be created.
                password: Administrator password required for Active Directory account creation, modification and deletion.
                preferred_dcs: Specifies the preferred domain controller (DC) records.
                svm: 
                username: Administrator username required for Active Directory account creation, modification and deletion.
            """

            kwargs = {}
            if discovered_servers is not None:
                kwargs["discovered_servers"] = discovered_servers
            if force_account_overwrite is not None:
                kwargs["force_account_overwrite"] = force_account_overwrite
            if fqdn is not None:
                kwargs["fqdn"] = fqdn
            if name is not None:
                kwargs["name"] = name
            if organizational_unit is not None:
                kwargs["organizational_unit"] = organizational_unit
            if password is not None:
                kwargs["password"] = password
            if preferred_dcs is not None:
                kwargs["preferred_dcs"] = preferred_dcs
            if svm is not None:
                kwargs["svm"] = svm
            if username is not None:
                kwargs["username"] = username

            resource = ActiveDirectory(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create ActiveDirectory: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Modifies the Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory modify`
### Important notes
* Patching Active Directory account is asynchronous. Response contains Task UUID and Link that can be queried to get the status.

### Learn more
* [`DOC /protocols/active-directory/{svm.uuid}`](#docs-NAS-protocols_active-directory_{svm.uuid})"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="active directory modify")
        async def active_directory_modify(
        ) -> ResourceTable:
            """Modify an instance of a ActiveDirectory resource

            Args:
                force_account_overwrite: If set to true and a machine account exists with the same name as specified in \"name\" in Active Directory, it will be overwritten and reused.
                query_force_account_overwrite: If set to true and a machine account exists with the same name as specified in \"name\" in Active Directory, it will be overwritten and reused.
                fqdn: Fully qualified domain name.
                query_fqdn: Fully qualified domain name.
                name: Active Directory (AD) account NetBIOS name.
                query_name: Active Directory (AD) account NetBIOS name.
                organizational_unit: Organizational unit under which the Active Directory account will be created.
                query_organizational_unit: Organizational unit under which the Active Directory account will be created.
                password: Administrator password required for Active Directory account creation, modification and deletion.
                query_password: Administrator password required for Active Directory account creation, modification and deletion.
                username: Administrator username required for Active Directory account creation, modification and deletion.
                query_username: Administrator username required for Active Directory account creation, modification and deletion.
            """

            kwargs = {}
            changes = {}
            if query_force_account_overwrite is not None:
                kwargs["force_account_overwrite"] = query_force_account_overwrite
            if query_fqdn is not None:
                kwargs["fqdn"] = query_fqdn
            if query_name is not None:
                kwargs["name"] = query_name
            if query_organizational_unit is not None:
                kwargs["organizational_unit"] = query_organizational_unit
            if query_password is not None:
                kwargs["password"] = query_password
            if query_username is not None:
                kwargs["username"] = query_username

            if force_account_overwrite is not None:
                changes["force_account_overwrite"] = force_account_overwrite
            if fqdn is not None:
                changes["fqdn"] = fqdn
            if name is not None:
                changes["name"] = name
            if organizational_unit is not None:
                changes["organizational_unit"] = organizational_unit
            if password is not None:
                changes["password"] = password
            if username is not None:
                changes["username"] = username

            if hasattr(ActiveDirectory, "find"):
                resource = ActiveDirectory.find(
                    **kwargs
                )
            else:
                resource = ActiveDirectory()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify ActiveDirectory: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the Active Directory account for a given SVM.
### Related ONTAP commands
* `vserver active-directory delete`
### Important notes
* Active Directory account deletion is asynchronous. Response contains Task UUID and Link that can be queried to get the status.

### Learn more
* [`DOC /protocols/active-directory/{svm.uuid}`](#docs-NAS-protocols_active-directory_{svm.uuid})"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="active directory delete")
        async def active_directory_delete(
        ) -> None:
            """Delete an instance of a ActiveDirectory resource

            Args:
                force_account_overwrite: If set to true and a machine account exists with the same name as specified in \"name\" in Active Directory, it will be overwritten and reused.
                fqdn: Fully qualified domain name.
                name: Active Directory (AD) account NetBIOS name.
                organizational_unit: Organizational unit under which the Active Directory account will be created.
                password: Administrator password required for Active Directory account creation, modification and deletion.
                username: Administrator username required for Active Directory account creation, modification and deletion.
            """

            kwargs = {}
            if force_account_overwrite is not None:
                kwargs["force_account_overwrite"] = force_account_overwrite
            if fqdn is not None:
                kwargs["fqdn"] = fqdn
            if name is not None:
                kwargs["name"] = name
            if organizational_unit is not None:
                kwargs["organizational_unit"] = organizational_unit
            if password is not None:
                kwargs["password"] = password
            if username is not None:
                kwargs["username"] = username

            if hasattr(ActiveDirectory, "find"):
                resource = ActiveDirectory.find(
                    **kwargs
                )
            else:
                resource = ActiveDirectory()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete ActiveDirectory: %s" % err)


