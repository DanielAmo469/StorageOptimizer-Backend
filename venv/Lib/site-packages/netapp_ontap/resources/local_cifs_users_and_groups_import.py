r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
Bulk import of the CIFS local users, groups and group membership information can be
done from the specified Uniform Resource Identifier (URI). This replaces the existing contents of the
CIFS local users, groups and group memberships. This API is used to bulk import from the specified URI,
get the status of the last import and to upload the import status to the specified URI.
## Retrieving import status of the last bulk import
The bulk-import GET endpoint retrieves the status of the last bulk-import operation of the specified SVM.
## Examples
### Retrieving the status of a successful bulk import
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LocalCifsUsersAndGroupsImport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LocalCifsUsersAndGroupsImport(
        **{"svm.uuid": "6de1d39d-1473-11ec-b0cf-0050568e4acc"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
LocalCifsUsersAndGroupsImport(
    {
        "elements_ignored": 0,
        "_links": {
            "self": {
                "href": "/api/protocols/cifs/users-and-groups/import/6de1d39d-1473-11ec-b0cf-0050568e4acc"
            }
        },
        "svm": {
            "name": "vs1",
            "_links": {
                "self": {"href": "/api/svm/svms/6de1d39d-1473-11ec-b0cf-0050568e4acc"}
            },
            "uuid": "6de1d39d-1473-11ec-b0cf-0050568e4acc",
        },
        "import_uri": {
            "path": "http://nbsweb.eng.btc.netapp.in/u/st/web/LUG_Import/Vserver1/user4.7z"
        },
        "state": "success",
        "detailed_status": {
            "code": "0",
            "message": "Operation completed successfully.",
        },
        "elements_imported": 2,
    }
)

```
</div>
</div>

### Retrieving the status of a bulk import that failed
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LocalCifsUsersAndGroupsImport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LocalCifsUsersAndGroupsImport(
        **{"svm.uuid": "6de1d39d-1473-11ec-b0cf-0050568e4acc"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
LocalCifsUsersAndGroupsImport(
    {
        "elements_ignored": 0,
        "_links": {
            "self": {
                "href": "/api/protocols/cifs/users-and-groups/import/6de1d39d-1473-11ec-b0cf-0050568e4acc"
            }
        },
        "svm": {
            "name": "vs1",
            "_links": {
                "self": {"href": "/api/svm/svms/6de1d39d-1473-11ec-b0cf-0050568e4acc"}
            },
            "uuid": "6de1d39d-1473-11ec-b0cf-0050568e4acc",
        },
        "import_uri": {
            "path": "http://nbsweb.eng.btc.netapp.in/u/st/web/LUG_Import/Vserver1/user5.7z"
        },
        "state": "success",
        "detailed_status": {
            "code": "655698",
            "message": "Failed parsing line 1 of the input file. Check syntax and contents.",
        },
        "elements_imported": 0,
    }
)

```
</div>
</div>

## Retrieving bulk import information for CIFS local users, groups, and group memberships
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LocalCifsUsersAndGroupsImport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LocalCifsUsersAndGroupsImport()
    resource.import_uri.username = "user1"
    resource.import_uri.password = "aaaa"
    resource.decryption_password = "cccc"
    resource.import_uri.path = "http://example.com/file1.7z"
    resource.post(hydrate=True)
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
LocalCifsUsersAndGroupsImport(
    {
        "import_uri": {
            "password": "aaaa",
            "path": "http://example.com/file1.7z",
            "username": "user1",
        },
        "decryption_password": "cccc",
    }
)

```
</div>
</div>

## Retrieving status upload information of the last import operation for the specified URI
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import LocalCifsUsersAndGroupsImport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = LocalCifsUsersAndGroupsImport(
        **{"svm.uuid": "6de1d39d-1473-11ec-b0cf-0050568e4acc"}
    )
    resource.status_uri.username = "user1"
    resource.status_uri.password = "aaaa"
    resource.status_uri.path = "http://example.com/fileupload.7z"
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


__all__ = ["LocalCifsUsersAndGroupsImport", "LocalCifsUsersAndGroupsImportSchema"]
__pdoc__ = {
    "LocalCifsUsersAndGroupsImportSchema.resource": False,
    "LocalCifsUsersAndGroupsImportSchema.opts": False,
    "LocalCifsUsersAndGroupsImport.local_cifs_users_and_groups_import_show": False,
    "LocalCifsUsersAndGroupsImport.local_cifs_users_and_groups_import_create": False,
    "LocalCifsUsersAndGroupsImport.local_cifs_users_and_groups_import_modify": False,
    "LocalCifsUsersAndGroupsImport.local_cifs_users_and_groups_import_delete": False,
}


class LocalCifsUsersAndGroupsImportSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the LocalCifsUsersAndGroupsImport object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the local_cifs_users_and_groups_import."""

    decryption_password = fields.Str(
        data_key="decryption_password",
        validate=len_validation(minimum=0, maximum=128),
    )
    r""" Password to decrypt the compressed file."""

    detailed_status = fields.Nested("netapp_ontap.models.detailed_status_code_message.DetailedStatusCodeMessageSchema", data_key="detailed_status", unknown=EXCLUDE)
    r""" The detailed_status field of the local_cifs_users_and_groups_import."""

    elements_ignored = Size(
        data_key="elements_ignored",
    )
    r""" Number of elements ignored."""

    elements_imported = Size(
        data_key="elements_imported",
    )
    r""" Number of elements successfully imported."""

    import_uri = fields.Nested("netapp_ontap.models.uniform_resource_identifier.UniformResourceIdentifierSchema", data_key="import_uri", unknown=EXCLUDE)
    r""" The import_uri field of the local_cifs_users_and_groups_import."""

    state = fields.Str(
        data_key="state",
        validate=enum_validation(['failed', 'success', 'success_with_warnings', 'in_progress', 'unknown']),
    )
    r""" Operation status.

Valid choices:

* failed
* success
* success_with_warnings
* in_progress
* unknown"""

    status_uri = fields.Nested("netapp_ontap.models.uniform_resource_identifier.UniformResourceIdentifierSchema", data_key="status_uri", unknown=EXCLUDE)
    r""" The status_uri field of the local_cifs_users_and_groups_import."""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the local_cifs_users_and_groups_import."""

    @property
    def resource(self):
        return LocalCifsUsersAndGroupsImport

    gettable_fields = [
        "links",
        "detailed_status",
        "elements_ignored",
        "elements_imported",
        "import_uri",
        "state",
        "svm.links",
        "svm.name",
        "svm.uuid",
    ]
    """links,detailed_status,elements_ignored,elements_imported,import_uri,state,svm.links,svm.name,svm.uuid,"""

    patchable_fields = [
        "status_uri",
        "svm.name",
        "svm.uuid",
    ]
    """status_uri,svm.name,svm.uuid,"""

    postable_fields = [
        "decryption_password",
        "import_uri",
        "svm.name",
        "svm.uuid",
    ]
    """decryption_password,import_uri,svm.name,svm.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in LocalCifsUsersAndGroupsImport.get_collection(fields=field)]
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
            raise NetAppRestError("LocalCifsUsersAndGroupsImport modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class LocalCifsUsersAndGroupsImport(Resource):
    """Allows interaction with LocalCifsUsersAndGroupsImport objects on the host"""

    _schema = LocalCifsUsersAndGroupsImportSchema
    _path = "/api/protocols/cifs/users-and-groups/bulk-import"
    _keys = ["svm.uuid"]


    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["LocalCifsUsersAndGroupsImport"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Upload the status of the bulk-import of the specified SVM to the specified URI.
### Important notes
* Only the status of the last bulk-import will be uploaded and not the status of the previous bulk-imports.
### Required properties
- status_uri.path - URI to which the status needs to be uploaded.
### Optional properties
- status_uri.username - Username of the specified URI.
- status_uri.password - Password of the specified URI.
### Related ONTAP commands
* `vserver cifs users-and-groups import get-status`

### Learn more
* [`DOC /protocols/cifs/users-and-groups/bulk-import/{svm.uuid}`](#docs-NAS-protocols_cifs_users-and-groups_bulk-import_{svm.uuid})"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["LocalCifsUsersAndGroupsImport"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["LocalCifsUsersAndGroupsImport"], NetAppResponse]:
        r"""Loads CIFS local users,groups and group memberships file from the specified URL.<br/>
### Important notes
Existing CIFS local users, groups, and group memberships will be replaced with the contents of the file.
### Required properties
- import_uri.path
- decryption_password
### Optional properties
- import_uri.username
- import_uri.password
### Related ONTAP commands
* `vserver cifs users-and-groups import load-from-uri`

### Learn more
* [`DOC /protocols/cifs/users-and-groups/bulk-import/{svm.uuid}`](#docs-NAS-protocols_cifs_users-and-groups_bulk-import_{svm.uuid})"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)



    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves information about the import operation status of the CIFS local users,
groups, and group memberships.
### Related ONTAP commands
* `vserver cifs users-and-groups import get-status`

### Learn more
* [`DOC /protocols/cifs/users-and-groups/bulk-import/{svm.uuid}`](#docs-NAS-protocols_cifs_users-and-groups_bulk-import_{svm.uuid})"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="local cifs users and groups import show")
        def local_cifs_users_and_groups_import_show(
            fields: List[str] = None,
        ) -> ResourceTable:
            """Fetch a single LocalCifsUsersAndGroupsImport resource

            Args:
                decryption_password: Password to decrypt the compressed file.
                elements_ignored: Number of elements ignored.
                elements_imported: Number of elements successfully imported.
                state: Operation status.
            """

            kwargs = {}
            if decryption_password is not None:
                kwargs["decryption_password"] = decryption_password
            if elements_ignored is not None:
                kwargs["elements_ignored"] = elements_ignored
            if elements_imported is not None:
                kwargs["elements_imported"] = elements_imported
            if state is not None:
                kwargs["state"] = state
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            resource = LocalCifsUsersAndGroupsImport(
                **kwargs
            )
            resource.get()
            return [resource]

    def post(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Loads CIFS local users,groups and group memberships file from the specified URL.<br/>
### Important notes
Existing CIFS local users, groups, and group memberships will be replaced with the contents of the file.
### Required properties
- import_uri.path
- decryption_password
### Optional properties
- import_uri.username
- import_uri.password
### Related ONTAP commands
* `vserver cifs users-and-groups import load-from-uri`

### Learn more
* [`DOC /protocols/cifs/users-and-groups/bulk-import/{svm.uuid}`](#docs-NAS-protocols_cifs_users-and-groups_bulk-import_{svm.uuid})"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="local cifs users and groups import create")
        async def local_cifs_users_and_groups_import_create(
        ) -> ResourceTable:
            """Create an instance of a LocalCifsUsersAndGroupsImport resource

            Args:
                links: 
                decryption_password: Password to decrypt the compressed file.
                detailed_status: 
                elements_ignored: Number of elements ignored.
                elements_imported: Number of elements successfully imported.
                import_uri: 
                state: Operation status.
                status_uri: 
                svm: 
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if decryption_password is not None:
                kwargs["decryption_password"] = decryption_password
            if detailed_status is not None:
                kwargs["detailed_status"] = detailed_status
            if elements_ignored is not None:
                kwargs["elements_ignored"] = elements_ignored
            if elements_imported is not None:
                kwargs["elements_imported"] = elements_imported
            if import_uri is not None:
                kwargs["import_uri"] = import_uri
            if state is not None:
                kwargs["state"] = state
            if status_uri is not None:
                kwargs["status_uri"] = status_uri
            if svm is not None:
                kwargs["svm"] = svm

            resource = LocalCifsUsersAndGroupsImport(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create LocalCifsUsersAndGroupsImport: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Upload the status of the bulk-import of the specified SVM to the specified URI.
### Important notes
* Only the status of the last bulk-import will be uploaded and not the status of the previous bulk-imports.
### Required properties
- status_uri.path - URI to which the status needs to be uploaded.
### Optional properties
- status_uri.username - Username of the specified URI.
- status_uri.password - Password of the specified URI.
### Related ONTAP commands
* `vserver cifs users-and-groups import get-status`

### Learn more
* [`DOC /protocols/cifs/users-and-groups/bulk-import/{svm.uuid}`](#docs-NAS-protocols_cifs_users-and-groups_bulk-import_{svm.uuid})"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="local cifs users and groups import modify")
        async def local_cifs_users_and_groups_import_modify(
        ) -> ResourceTable:
            """Modify an instance of a LocalCifsUsersAndGroupsImport resource

            Args:
                decryption_password: Password to decrypt the compressed file.
                query_decryption_password: Password to decrypt the compressed file.
                elements_ignored: Number of elements ignored.
                query_elements_ignored: Number of elements ignored.
                elements_imported: Number of elements successfully imported.
                query_elements_imported: Number of elements successfully imported.
                state: Operation status.
                query_state: Operation status.
            """

            kwargs = {}
            changes = {}
            if query_decryption_password is not None:
                kwargs["decryption_password"] = query_decryption_password
            if query_elements_ignored is not None:
                kwargs["elements_ignored"] = query_elements_ignored
            if query_elements_imported is not None:
                kwargs["elements_imported"] = query_elements_imported
            if query_state is not None:
                kwargs["state"] = query_state

            if decryption_password is not None:
                changes["decryption_password"] = decryption_password
            if elements_ignored is not None:
                changes["elements_ignored"] = elements_ignored
            if elements_imported is not None:
                changes["elements_imported"] = elements_imported
            if state is not None:
                changes["state"] = state

            if hasattr(LocalCifsUsersAndGroupsImport, "find"):
                resource = LocalCifsUsersAndGroupsImport.find(
                    **kwargs
                )
            else:
                resource = LocalCifsUsersAndGroupsImport()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify LocalCifsUsersAndGroupsImport: %s" % err)



