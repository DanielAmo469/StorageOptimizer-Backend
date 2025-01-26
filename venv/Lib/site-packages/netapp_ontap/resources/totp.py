r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API configures the TOTP profile for user accounts.
Specify the owner UUID and the account user name. The owner UUID corresponds to the UUID of the SVM containing the user account associated with the TOTP profile and can be obtained from the response body of the GET request performed on the API “/api/svm/svms".
## Examples
### Retrieving the specific configured TOTP profile for user accounts
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Totp

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Totp(
        **{
            "account.name": "pubuser4",
            "owner.uuid": "513a78c7-8c13-11e9-8f78-005056bbf6ac",
        }
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
Totp(
    {
        "scope": "cluster",
        "_links": {
            "self": {
                "href": "/api/security/login/totps/b009a9e7-4081-b576-7575-ada21efcaf16/pubuser2"
            }
        },
        "sha_fingerprint": "21364f5417600e3d9d6a7ac6c05dd244aed9f15dce6786a2c89399a41ff0fdb0",
        "owner": {
            "name": "Default",
            "_links": {
                "self": {"href": "/api/svm/svms/b009a9e7-4081-b576-7575-ada21efcaf16"}
            },
            "uuid": "b009a9e7-4081-b576-7575-ada21efcaf16",
        },
        "account": {
            "_links": {
                "self": {
                    "href": "/api/security/accounts/b009a9e7-4081-b576-7575-ada21efcaf16/pubuser2"
                }
            },
            "name": "pubuser2",
        },
    }
)

```
</div>
</div>

### Modifying the TOTP profile for a user account
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Totp

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Totp(
        **{
            "account.name": "ysadmin",
            "owner.uuid": "6865196a-8b59-11ed-874c-0050568e36ed",
        }
    )
    resource.comment = "Testing"
    resource.enabled = False
    resource.patch()

```

### Deleting the TOTP profile for user accounts
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Totp

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Totp(
        **{
            "account.name": "pubuser1",
            "owner.uuid": "d49de271-8c11-11e9-8f78-005056bbf6ac",
        }
    )
    resource.delete()

```
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


__all__ = ["Totp", "TotpSchema"]
__pdoc__ = {
    "TotpSchema.resource": False,
    "TotpSchema.opts": False,
    "Totp.totp_show": False,
    "Totp.totp_create": False,
    "Totp.totp_modify": False,
    "Totp.totp_delete": False,
}


class TotpSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Totp object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the totp."""

    account = fields.Nested("netapp_ontap.resources.account.AccountSchema", data_key="account", unknown=EXCLUDE)
    r""" The account field of the totp."""

    comment = fields.Str(
        data_key="comment",
    )
    r""" Optional comment for the TOTP profile."""

    enabled = fields.Boolean(
        data_key="enabled",
    )
    r""" Status of the TOTP profile.

Example: false"""

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="owner", unknown=EXCLUDE)
    r""" The owner field of the totp."""

    scope = fields.Str(
        data_key="scope",
        validate=enum_validation(['cluster', 'svm']),
    )
    r""" Scope of the entity. Set to "cluster" for cluster owned objects and to "svm" for SVM owned objects.

Valid choices:

* cluster
* svm"""

    sha_fingerprint = fields.Str(
        data_key="sha_fingerprint",
    )
    r""" SHA fingerprint for the TOTP secret key."""

    @property
    def resource(self):
        return Totp

    gettable_fields = [
        "links",
        "account.links",
        "account.name",
        "comment",
        "enabled",
        "owner.links",
        "owner.name",
        "owner.uuid",
        "scope",
        "sha_fingerprint",
    ]
    """links,account.links,account.name,comment,enabled,owner.links,owner.name,owner.uuid,scope,sha_fingerprint,"""

    patchable_fields = [
        "account.links",
        "account.name",
        "comment",
        "enabled",
    ]
    """account.links,account.name,comment,enabled,"""

    postable_fields = [
        "account.links",
        "account.name",
        "comment",
        "owner.name",
        "owner.uuid",
    ]
    """account.links,account.name,comment,owner.name,owner.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in Totp.get_collection(fields=field)]
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
            raise NetAppRestError("Totp modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class Totp(Resource):
    r""" TOTP profile for the user account used to access SSH. """

    _schema = TotpSchema
    _path = "/api/security/login/totps"
    _keys = ["owner.uuid", "account.name"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the TOTP profiles configured for user accounts.
### Related ONTAP commands
* `security login totp show`
### Learn more
* [`DOC /security/login/totps`](#docs-security-security_login_totps)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="totp show")
        def totp_show(
            fields: List[Choices.define(["comment", "enabled", "scope", "sha_fingerprint", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of Totp resources

            Args:
                comment: Optional comment for the TOTP profile.
                enabled: Status of the TOTP profile.
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: SHA fingerprint for the TOTP secret key.
            """

            kwargs = {}
            if comment is not None:
                kwargs["comment"] = comment
            if enabled is not None:
                kwargs["enabled"] = enabled
            if scope is not None:
                kwargs["scope"] = scope
            if sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = sha_fingerprint
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return Totp.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all Totp resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["Totp"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a TOTP user account.
### Related ONTAP commands
* `security login totp modify`
### Learn more
* [`DOC /security/login/totps/{owner.uuid}/{account.name}`](#docs-security-security_login_totps_{owner.uuid}_{account.name})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._patch_collection(
            body, *args, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    patch_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch_collection.__doc__)

    @classmethod
    def post_collection(
        cls,
        records: Iterable["Totp"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["Totp"], NetAppResponse]:
        r"""Creates a TOTP profile for a user account.
### Required properties
* `owner.uuid` - Account owner UUID.
* `account.name` - Account user name.
### Related ONTAP commands
* `security login totp create`
### Learn more
* [`DOC /security/login/totps`](#docs-security-security_login_totps)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._post_collection(
            records, *args, hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    post_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post_collection.__doc__)

    @classmethod
    def delete_collection(
        cls,
        *args,
        records: Iterable["Totp"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the TOTP profile for a user account.
### Related ONTAP commands
* `security login totp delete`
### Learn more
* [`DOC /security/login/totps/{owner.uuid}/{account.name}`](#docs-security-security_login_totps_{owner.uuid}_{account.name})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the TOTP profiles configured for user accounts.
### Related ONTAP commands
* `security login totp show`
### Learn more
* [`DOC /security/login/totps`](#docs-security-security_login_totps)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the TOTP profile configured for a user account.
### Related ONTAP commands
* `security login totp show`
### Learn more
* [`DOC /security/login/totps/{owner.uuid}/{account.name}`](#docs-security-security_login_totps_{owner.uuid}_{account.name})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
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
        r"""Creates a TOTP profile for a user account.
### Required properties
* `owner.uuid` - Account owner UUID.
* `account.name` - Account user name.
### Related ONTAP commands
* `security login totp create`
### Learn more
* [`DOC /security/login/totps`](#docs-security-security_login_totps)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="totp create")
        async def totp_create(
        ) -> ResourceTable:
            """Create an instance of a Totp resource

            Args:
                links: 
                account: 
                comment: Optional comment for the TOTP profile.
                enabled: Status of the TOTP profile.
                owner: 
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: SHA fingerprint for the TOTP secret key.
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if account is not None:
                kwargs["account"] = account
            if comment is not None:
                kwargs["comment"] = comment
            if enabled is not None:
                kwargs["enabled"] = enabled
            if owner is not None:
                kwargs["owner"] = owner
            if scope is not None:
                kwargs["scope"] = scope
            if sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = sha_fingerprint

            resource = Totp(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create Totp: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates a TOTP user account.
### Related ONTAP commands
* `security login totp modify`
### Learn more
* [`DOC /security/login/totps/{owner.uuid}/{account.name}`](#docs-security-security_login_totps_{owner.uuid}_{account.name})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="totp modify")
        async def totp_modify(
        ) -> ResourceTable:
            """Modify an instance of a Totp resource

            Args:
                comment: Optional comment for the TOTP profile.
                query_comment: Optional comment for the TOTP profile.
                enabled: Status of the TOTP profile.
                query_enabled: Status of the TOTP profile.
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                query_scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: SHA fingerprint for the TOTP secret key.
                query_sha_fingerprint: SHA fingerprint for the TOTP secret key.
            """

            kwargs = {}
            changes = {}
            if query_comment is not None:
                kwargs["comment"] = query_comment
            if query_enabled is not None:
                kwargs["enabled"] = query_enabled
            if query_scope is not None:
                kwargs["scope"] = query_scope
            if query_sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = query_sha_fingerprint

            if comment is not None:
                changes["comment"] = comment
            if enabled is not None:
                changes["enabled"] = enabled
            if scope is not None:
                changes["scope"] = scope
            if sha_fingerprint is not None:
                changes["sha_fingerprint"] = sha_fingerprint

            if hasattr(Totp, "find"):
                resource = Totp.find(
                    **kwargs
                )
            else:
                resource = Totp()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify Totp: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the TOTP profile for a user account.
### Related ONTAP commands
* `security login totp delete`
### Learn more
* [`DOC /security/login/totps/{owner.uuid}/{account.name}`](#docs-security-security_login_totps_{owner.uuid}_{account.name})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="totp delete")
        async def totp_delete(
        ) -> None:
            """Delete an instance of a Totp resource

            Args:
                comment: Optional comment for the TOTP profile.
                enabled: Status of the TOTP profile.
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: SHA fingerprint for the TOTP secret key.
            """

            kwargs = {}
            if comment is not None:
                kwargs["comment"] = comment
            if enabled is not None:
                kwargs["enabled"] = enabled
            if scope is not None:
                kwargs["scope"] = scope
            if sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = sha_fingerprint

            if hasattr(Totp, "find"):
                resource = Totp.find(
                    **kwargs
                )
            else:
                resource = Totp()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete Totp: %s" % err)


