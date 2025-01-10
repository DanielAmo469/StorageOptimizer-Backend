r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
This API configures the public keys for end-user (non-cluster admin) accounts.
Specify the owner UUID, the user account name, and the index in the URI path. The owner UUID corresponds to the UUID of the SVM containing the user account associated with the public key and can be obtained from the response body of the GET request performed on the API “/api/svm/svms".<br/> The index value corresponds to the public key that needs to be modified or deleted (it is possible to create more than one public key for the same user account).
## Examples
### Retrieving the specific configured public key for user accounts
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Publickey

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Publickey(
        index=0,
        **{
            "account.name": "pubuser4",
            "owner.uuid": "513a78c7-8c13-11e9-8f78-005056bbf6ac",
        }
    )
    resource.get()
    print(resource)

```

### Updating the public key, certificate, and comment for user accounts
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Publickey

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Publickey(
        index=0,
        **{
            "account.name": "pubuser1",
            "owner.uuid": "d49de271-8c11-11e9-8f78-005056bbf6ac",
        }
    )
    resource.comment = "Cserver-modification"
    resource.certificate = (
        "-----BEGIN CERTIFICATE-----"
        "MIIFrTCCA5WgAwIBAgICEAMwDQYJKoZIhvcNAQELBQAwYDELMAkGA1UEBhMCVVMx"
        "CzAJBgNVBAgMAk5DMQwwCgYDVQQHDANSVFAxDzANBgNVBAoMBk5FVEFQUDENMAsG"
        "A1UECwwETlRBUDEWMBQGA1UEAwwNTlRBUC1JTlRFUkNBMjAeFw0yMzAxMTkwOTE4"
        "MzBaFw0yNDAxMjkwOTE4MzBaMFcxCzAJBgNVBAYTAklOMQswCQYDVQQIDAJLQTEM"
        "MAoGA1UEBwwDQkxSMQ0wCwYDVQQKDAROVEFQMQ0wCwYDVQQLDAROVEFQMQ8wDQYD"
        "VQQDDAZNWU5UQVAwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDfkWQD"
        "4kQcInzLQh95eNMXOP6AK9DIzM1e5V7350xTiWmrmiqREh96Asms4RxOHTI4Q1ox"
        "ghn3NugjWy/y9aCao+Uz6nIG8gAP+NIYb3TU/WeGJFKF6fRJgaZxIzBjla3x1QQ5"
        "rCWZMPuEiKZeBtnyHnoz6g3d5Cz4Ahu2mmHUDbAah25nNuYA9vbroP4GPtE4KQYQ"
        "2lKtXnw8UKvyTYBOU3KzM2PP+lhtNmh3l/rgFhx99x1P6x8I8c6xRRQIjfIhHH9n"
        "8mLkElc3SMSeRNLIQn8JSd9gly6FyHDF2jsPWdRjTlPyvGeN+LNUsBrBgmeyuFvA"
        "Tq0/7lavqoNiwA4dAgMBAAGjggF4MIIBdDAJBgNVHRMEAjAAMBEGCWCGSAGG+EIB"
        "AQQEAwIGQDAzBglghkgBhvhCAQ0EJhYkT3BlblNTTCBHZW5lcmF0ZWQgU2VydmVy"
        "IENlcnRpZmljYXRlMB0GA1UdDgQWBBQkJGop1KmP0D5jkblSGk3nSGHf5jCBiwYD"
        "VR0jBIGDMIGAgBQqjApAoQETk23RqM0Fo7u60SsmL6FkpGIwYDELMAkGA1UEBhMC"
        "VVMxCzAJBgNVBAgMAk5DMQwwCgYDVQQHDANSVFAxDzANBgNVBAoMBk5FVEFQUDEN"
        "MAsGA1UECwwETlRBUDEWMBQGA1UEAwwNTlRBUC1JTlRFUkNBMYICEAAwDgYDVR0P"
        "AQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBME0GCCsGAQUFBwEBBEEwPzA9"
        "BggrBgEFBQcwAYYxaHR0cDovL3Njc3ByMjY5Mjc4OTAyMS5nZGwuZW5nbGFiLm5l"
        "dGFwcC5jb206MjU2MDANBgkqhkiG9w0BAQsFAAOCAgEASSs8BR96qNipv4X8ZS49"
        "hW5MpkuQmHg2E7ICXYPP+r0qHeAa0fVpstLoju7ICo1HyfszwlncO8X2V37cQsCB"
        "MsMq1THVhKExPuAwUjTk6aP6kiun8Werr7rOqFKheZDkCxIMQ0E2mK+O5z6wZaqc"
        "Oa1o4jmAEDUvLBYLYxa0qXa1EunLpOOJTg0fkCW8SOwGDT7CWhpk1AiqivnGnsaz"
        "hN54gPbinI6La9elEfbNJSOLQUGzvp9nhkFGNssx5tl0Ij+qzxV6DrzbY8qAeCH2"
        "rZnasMILUGISQC1LvxxeGcZ7da4AX3V8/ixHeKoUsk5kA+ucHEB+GP15L0KGU5xa"
        "Y/Uy7Uoh1GRPmvILelxzf2jK+z4x8hudJ9TUrskrLHkrsAm68eW5IikIJmQsCBiM"
        "ioGib6tWl250etSiC9byQ48W99yOlyShe8EQStogOeshXJfMyY7VZa0YA/4KMtvi"
        "O+fxF6LdeFMeu0qxvYLYnIbNPmc2ohGrZwffnL/Kc9s9RF5dk9bjchCKuL3+bdBm"
        "IdcvjGi1gGHzgvsg7W54/ctwFH/qW5N68SE7JCv0DtydjUhtlU34I1RfrJD72L3X"
        "LAb0KlLG92Oun5psy49vprr143X7eOlGB4TNjUsXW9lNP/R8J3o1ZNnoZq7E32XI"
        "tsi/5Ttkq7aT975alerJoAU="
        "-----END CERTIFICATE-----"
    )
    resource.public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDfkWQD4kQcInzLQh95eNMXOP6AK9DIzM1e5V7350xTiWmrmiqREh96Asms4RxOHTI4Q1oxghn3NugjWy/y9aCao+Uz6nIG8gAP+NIYb3TU/WeGJFKF6fRJgaZxIzBjla3x1QQ5rCWZMPuEiKZeBtnyHnoz6g3d5Cz4Ahu2mmHUDbAah25nNuYA9vbroP4GPtE4KQYQ2lKtXnw8UKvyTYBOU3KzM2PP+lhtNmh3l/rgFhx99x1P6x8I8c6xRRQIjfIhHH9n8mLkElc3SMSeRNLIQn8JSd9gly6FyHDF2jsPWdRjTlPyvGeN+LNUsBrBgmeyuFvATq0/7lavqoNiwA4d"
    resource.patch()

```

### Deleting the public key for user accounts
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import Publickey

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = Publickey(
        index=0,
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


__all__ = ["Publickey", "PublickeySchema"]
__pdoc__ = {
    "PublickeySchema.resource": False,
    "PublickeySchema.opts": False,
    "Publickey.publickey_show": False,
    "Publickey.publickey_create": False,
    "Publickey.publickey_modify": False,
    "Publickey.publickey_delete": False,
}


class PublickeySchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the Publickey object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the publickey."""

    account = fields.Nested("netapp_ontap.resources.account.AccountSchema", data_key="account", unknown=EXCLUDE)
    r""" The account field of the publickey."""

    certificate = fields.Str(
        data_key="certificate",
    )
    r""" Optional certificate for the public key."""

    certificate_details = fields.Str(
        data_key="certificate_details",
    )
    r""" The details present in the certificate (READONLY)."""

    certificate_expired = fields.Str(
        data_key="certificate_expired",
    )
    r""" The expiration details of the certificate (READONLY)."""

    certificate_revoked = fields.Str(
        data_key="certificate_revoked",
    )
    r""" The revocation details of the certificate (READONLY)."""

    comment = fields.Str(
        data_key="comment",
    )
    r""" Optional comment for the public key."""

    index = Size(
        data_key="index",
        validate=integer_validation(minimum=0, maximum=99),
    )
    r""" Index number for the public key (where there are multiple keys for the same account)."""

    obfuscated_fingerprint = fields.Str(
        data_key="obfuscated_fingerprint",
    )
    r""" The obfuscated fingerprint for the public key (READONLY)."""

    owner = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="owner", unknown=EXCLUDE)
    r""" The owner field of the publickey."""

    public_key = fields.Str(
        data_key="public_key",
    )
    r""" The public key"""

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
    r""" The SHA fingerprint for the public key (READONLY)."""

    @property
    def resource(self):
        return Publickey

    gettable_fields = [
        "links",
        "account.links",
        "account.name",
        "certificate",
        "certificate_details",
        "certificate_expired",
        "certificate_revoked",
        "comment",
        "index",
        "obfuscated_fingerprint",
        "owner.links",
        "owner.name",
        "owner.uuid",
        "public_key",
        "scope",
        "sha_fingerprint",
    ]
    """links,account.links,account.name,certificate,certificate_details,certificate_expired,certificate_revoked,comment,index,obfuscated_fingerprint,owner.links,owner.name,owner.uuid,public_key,scope,sha_fingerprint,"""

    patchable_fields = [
        "account.links",
        "account.name",
        "certificate",
        "comment",
        "public_key",
    ]
    """account.links,account.name,certificate,comment,public_key,"""

    postable_fields = [
        "account.links",
        "account.name",
        "certificate",
        "comment",
        "index",
        "owner.name",
        "owner.uuid",
        "public_key",
    ]
    """account.links,account.name,certificate,comment,index,owner.name,owner.uuid,public_key,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in Publickey.get_collection(fields=field)]
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
            raise NetAppRestError("Publickey modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class Publickey(Resource):
    r""" The public key for the user account (to access SSH). """

    _schema = PublickeySchema
    _path = "/api/security/authentication/publickeys"
    _keys = ["owner.uuid", "account.name", "index"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the public keys configured for user accounts.
### Related ONTAP commands
* `security login publickey show`
### Learn more
* [`DOC /security/authentication/publickeys`](#docs-security-security_authentication_publickeys)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="publickey show")
        def publickey_show(
            fields: List[Choices.define(["certificate", "certificate_details", "certificate_expired", "certificate_revoked", "comment", "index", "obfuscated_fingerprint", "public_key", "scope", "sha_fingerprint", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of Publickey resources

            Args:
                certificate: Optional certificate for the public key.
                certificate_details: The details present in the certificate (READONLY).
                certificate_expired: The expiration details of the certificate (READONLY).
                certificate_revoked: The revocation details of the certificate (READONLY).
                comment: Optional comment for the public key.
                index: Index number for the public key (where there are multiple keys for the same account).
                obfuscated_fingerprint: The obfuscated fingerprint for the public key (READONLY).
                public_key: The public key
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: The SHA fingerprint for the public key (READONLY).
            """

            kwargs = {}
            if certificate is not None:
                kwargs["certificate"] = certificate
            if certificate_details is not None:
                kwargs["certificate_details"] = certificate_details
            if certificate_expired is not None:
                kwargs["certificate_expired"] = certificate_expired
            if certificate_revoked is not None:
                kwargs["certificate_revoked"] = certificate_revoked
            if comment is not None:
                kwargs["comment"] = comment
            if index is not None:
                kwargs["index"] = index
            if obfuscated_fingerprint is not None:
                kwargs["obfuscated_fingerprint"] = obfuscated_fingerprint
            if public_key is not None:
                kwargs["public_key"] = public_key
            if scope is not None:
                kwargs["scope"] = scope
            if sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = sha_fingerprint
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return Publickey.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all Publickey resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)

    @classmethod
    def patch_collection(
        cls,
        body: dict,
        *args,
        records: Iterable["Publickey"] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the public key and/or certificate for a user account.
### Related ONTAP commands
* `security login publickey modify`
### Learn more
* [`DOC /security/authentication/publickeys/{owner.uuid}/{account.name}/{index}`](#docs-security-security_authentication_publickeys_{owner.uuid}_{account.name}_{index})
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
        records: Iterable["Publickey"],
        *args,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> Union[List["Publickey"], NetAppResponse]:
        r"""Creates a public key along with an optional certificate for a user account.
### Required properties
* `owner.uuid` - UUID of the account owner.
* `name` - User account name.
* `index` - Index number for the public key (where there are multiple keys for the same account).
* `public_key` - The publickey details for the creation of the user account.
### Optional properties
* `comment` - Comment text for the public key.
* `certificate` - The certificate in PEM format.
### Related ONTAP commands
* `security login publickey create`
### Learn more
* [`DOC /security/authentication/publickeys`](#docs-security-security_authentication_publickeys)
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
        records: Iterable["Publickey"] = None,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        connection: HostConnection = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the public key for a user account.
### Related ONTAP commands
* `security login publickey delete`
### Learn more
* [`DOC /security/authentication/publickeys/{owner.uuid}/{account.name}/{index}`](#docs-security-security_authentication_publickeys_{owner.uuid}_{account.name}_{index})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._delete_collection(
            *args, body=body, records=records, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, connection=connection, **kwargs
        )

    delete_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete_collection.__doc__)

    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the public keys configured for user accounts.
### Related ONTAP commands
* `security login publickey show`
### Learn more
* [`DOC /security/authentication/publickeys`](#docs-security-security_authentication_publickeys)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves the public keys configured for a user account.
### Related ONTAP commands
* `security login publickey show`
### Learn more
* [`DOC /security/authentication/publickeys/{owner.uuid}/{account.name}/{index}`](#docs-security-security_authentication_publickeys_{owner.uuid}_{account.name}_{index})
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
        r"""Creates a public key along with an optional certificate for a user account.
### Required properties
* `owner.uuid` - UUID of the account owner.
* `name` - User account name.
* `index` - Index number for the public key (where there are multiple keys for the same account).
* `public_key` - The publickey details for the creation of the user account.
### Optional properties
* `comment` - Comment text for the public key.
* `certificate` - The certificate in PEM format.
### Related ONTAP commands
* `security login publickey create`
### Learn more
* [`DOC /security/authentication/publickeys`](#docs-security-security_authentication_publickeys)
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._post(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    post.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._post.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="publickey create")
        async def publickey_create(
        ) -> ResourceTable:
            """Create an instance of a Publickey resource

            Args:
                links: 
                account: 
                certificate: Optional certificate for the public key.
                certificate_details: The details present in the certificate (READONLY).
                certificate_expired: The expiration details of the certificate (READONLY).
                certificate_revoked: The revocation details of the certificate (READONLY).
                comment: Optional comment for the public key.
                index: Index number for the public key (where there are multiple keys for the same account).
                obfuscated_fingerprint: The obfuscated fingerprint for the public key (READONLY).
                owner: 
                public_key: The public key
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: The SHA fingerprint for the public key (READONLY).
            """

            kwargs = {}
            if links is not None:
                kwargs["links"] = links
            if account is not None:
                kwargs["account"] = account
            if certificate is not None:
                kwargs["certificate"] = certificate
            if certificate_details is not None:
                kwargs["certificate_details"] = certificate_details
            if certificate_expired is not None:
                kwargs["certificate_expired"] = certificate_expired
            if certificate_revoked is not None:
                kwargs["certificate_revoked"] = certificate_revoked
            if comment is not None:
                kwargs["comment"] = comment
            if index is not None:
                kwargs["index"] = index
            if obfuscated_fingerprint is not None:
                kwargs["obfuscated_fingerprint"] = obfuscated_fingerprint
            if owner is not None:
                kwargs["owner"] = owner
            if public_key is not None:
                kwargs["public_key"] = public_key
            if scope is not None:
                kwargs["scope"] = scope
            if sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = sha_fingerprint

            resource = Publickey(
                **kwargs
            )
            try:
                response = resource.post(hydrate=True, poll=False)
                await _wait_for_job(response)
                resource.get()
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to create Publickey: %s" % err)
            return [resource]

    def patch(
        self,
        hydrate: bool = False,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Updates the public key and/or certificate for a user account.
### Related ONTAP commands
* `security login publickey modify`
### Learn more
* [`DOC /security/authentication/publickeys/{owner.uuid}/{account.name}/{index}`](#docs-security-security_authentication_publickeys_{owner.uuid}_{account.name}_{index})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._patch(
            hydrate=hydrate, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    patch.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._patch.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="publickey modify")
        async def publickey_modify(
        ) -> ResourceTable:
            """Modify an instance of a Publickey resource

            Args:
                certificate: Optional certificate for the public key.
                query_certificate: Optional certificate for the public key.
                certificate_details: The details present in the certificate (READONLY).
                query_certificate_details: The details present in the certificate (READONLY).
                certificate_expired: The expiration details of the certificate (READONLY).
                query_certificate_expired: The expiration details of the certificate (READONLY).
                certificate_revoked: The revocation details of the certificate (READONLY).
                query_certificate_revoked: The revocation details of the certificate (READONLY).
                comment: Optional comment for the public key.
                query_comment: Optional comment for the public key.
                index: Index number for the public key (where there are multiple keys for the same account).
                query_index: Index number for the public key (where there are multiple keys for the same account).
                obfuscated_fingerprint: The obfuscated fingerprint for the public key (READONLY).
                query_obfuscated_fingerprint: The obfuscated fingerprint for the public key (READONLY).
                public_key: The public key
                query_public_key: The public key
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                query_scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: The SHA fingerprint for the public key (READONLY).
                query_sha_fingerprint: The SHA fingerprint for the public key (READONLY).
            """

            kwargs = {}
            changes = {}
            if query_certificate is not None:
                kwargs["certificate"] = query_certificate
            if query_certificate_details is not None:
                kwargs["certificate_details"] = query_certificate_details
            if query_certificate_expired is not None:
                kwargs["certificate_expired"] = query_certificate_expired
            if query_certificate_revoked is not None:
                kwargs["certificate_revoked"] = query_certificate_revoked
            if query_comment is not None:
                kwargs["comment"] = query_comment
            if query_index is not None:
                kwargs["index"] = query_index
            if query_obfuscated_fingerprint is not None:
                kwargs["obfuscated_fingerprint"] = query_obfuscated_fingerprint
            if query_public_key is not None:
                kwargs["public_key"] = query_public_key
            if query_scope is not None:
                kwargs["scope"] = query_scope
            if query_sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = query_sha_fingerprint

            if certificate is not None:
                changes["certificate"] = certificate
            if certificate_details is not None:
                changes["certificate_details"] = certificate_details
            if certificate_expired is not None:
                changes["certificate_expired"] = certificate_expired
            if certificate_revoked is not None:
                changes["certificate_revoked"] = certificate_revoked
            if comment is not None:
                changes["comment"] = comment
            if index is not None:
                changes["index"] = index
            if obfuscated_fingerprint is not None:
                changes["obfuscated_fingerprint"] = obfuscated_fingerprint
            if public_key is not None:
                changes["public_key"] = public_key
            if scope is not None:
                changes["scope"] = scope
            if sha_fingerprint is not None:
                changes["sha_fingerprint"] = sha_fingerprint

            if hasattr(Publickey, "find"):
                resource = Publickey.find(
                    **kwargs
                )
            else:
                resource = Publickey()
            try:
                for key, value in changes.items():
                    setattr(resource, key, value)
                response = resource.patch(poll=False)
                await _wait_for_job(response)
                resource.get(fields=",".join(changes.keys()))
                return [resource]
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to modify Publickey: %s" % err)

    def delete(
        self,
        body: Union[Resource, dict] = None,
        poll: bool = True,
        poll_interval: Optional[int] = None,
        poll_timeout: Optional[int] = None,
        **kwargs
    ) -> NetAppResponse:
        r"""Deletes the public key for a user account.
### Related ONTAP commands
* `security login publickey delete`
### Learn more
* [`DOC /security/authentication/publickeys/{owner.uuid}/{account.name}/{index}`](#docs-security-security_authentication_publickeys_{owner.uuid}_{account.name}_{index})
* [`DOC /security/accounts`](#docs-security-security_accounts)
"""
        return super()._delete(
            body=body, poll=poll, poll_interval=poll_interval,
            poll_timeout=poll_timeout, **kwargs
        )

    delete.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._delete.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="publickey delete")
        async def publickey_delete(
        ) -> None:
            """Delete an instance of a Publickey resource

            Args:
                certificate: Optional certificate for the public key.
                certificate_details: The details present in the certificate (READONLY).
                certificate_expired: The expiration details of the certificate (READONLY).
                certificate_revoked: The revocation details of the certificate (READONLY).
                comment: Optional comment for the public key.
                index: Index number for the public key (where there are multiple keys for the same account).
                obfuscated_fingerprint: The obfuscated fingerprint for the public key (READONLY).
                public_key: The public key
                scope: Scope of the entity. Set to \"cluster\" for cluster owned objects and to \"svm\" for SVM owned objects.
                sha_fingerprint: The SHA fingerprint for the public key (READONLY).
            """

            kwargs = {}
            if certificate is not None:
                kwargs["certificate"] = certificate
            if certificate_details is not None:
                kwargs["certificate_details"] = certificate_details
            if certificate_expired is not None:
                kwargs["certificate_expired"] = certificate_expired
            if certificate_revoked is not None:
                kwargs["certificate_revoked"] = certificate_revoked
            if comment is not None:
                kwargs["comment"] = comment
            if index is not None:
                kwargs["index"] = index
            if obfuscated_fingerprint is not None:
                kwargs["obfuscated_fingerprint"] = obfuscated_fingerprint
            if public_key is not None:
                kwargs["public_key"] = public_key
            if scope is not None:
                kwargs["scope"] = scope
            if sha_fingerprint is not None:
                kwargs["sha_fingerprint"] = sha_fingerprint

            if hasattr(Publickey, "find"):
                resource = Publickey.find(
                    **kwargs
                )
            else:
                resource = Publickey()
            try:
                response = resource.delete(poll=False)
                await _wait_for_job(response)
            except NetAppRestError as err:
                raise ReclineCommandError("Unable to delete Publickey: %s" % err)


