r"""
Copyright &copy; 2023 NetApp Inc.
All rights reserved.

This file has been automatically generated based on the ONTAP REST API documentation.

## Overview
Quota reports provide the current file and space consumption for a user, group, or qtree in a FlexVol or a FlexGroup volume.
## Quota report APIs
The following APIs can be used to retrieve quota reports associated with a volume in ONTAP.

* GET       /api/storage/quota/reports
* GET       /api/storage/quota/reports/{volume_uuid}/{index}
## Examples
### Retrieving all the quota report records
This API is used to retrieve all the quota report records. <br/>
The following example shows how to retrieve quota report records for all FlexVol volumes and FlexGroup volumes.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(QuotaReport.get_collection()))

```
<div class="try_it_out">
<input id="example0_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example0_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example0_result" class="try_it_out_content">
```
[
    QuotaReport(
        {
            "index": 0,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/314a328f-502d-11e9-8771-005056a7f717/0"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fg",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/314a328f-502d-11e9-8771-005056a7f717"
                    }
                },
                "uuid": "314a328f-502d-11e9-8771-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 1152921504606846976,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/314a328f-502d-11e9-8771-005056a7f717/1152921504606846976"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fg",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/314a328f-502d-11e9-8771-005056a7f717"
                    }
                },
                "uuid": "314a328f-502d-11e9-8771-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 3458764513820540928,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/314a328f-502d-11e9-8771-005056a7f717/3458764513820540928"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fg",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/314a328f-502d-11e9-8771-005056a7f717"
                    }
                },
                "uuid": "314a328f-502d-11e9-8771-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 4611686018427387904,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/314a328f-502d-11e9-8771-005056a7f717/4611686018427387904"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fg",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/314a328f-502d-11e9-8771-005056a7f717"
                    }
                },
                "uuid": "314a328f-502d-11e9-8771-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 5764607523034234880,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/314a328f-502d-11e9-8771-005056a7f717/5764607523034234880"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fg",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/314a328f-502d-11e9-8771-005056a7f717"
                    }
                },
                "uuid": "314a328f-502d-11e9-8771-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 0,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/0"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 281474976710656,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/281474976710656"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 1152921504606846976,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/1152921504606846976"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 1153202979583557632,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/1153202979583557632"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 2305843013508661248,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/2305843013508661248"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 3458764513820540928,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/3458764513820540928"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 3459045988797251584,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/3459045988797251584"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 4611686018427387904,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/4611686018427387904"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 4611967493404098560,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/4611967493404098560"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
    QuotaReport(
        {
            "index": 5764607523034234880,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cb20da45-4f6b-11e9-9a71-005056a7f717/5764607523034234880"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/b68f961b-4cee-11e9-930a-005056a7f717"
                    }
                },
                "uuid": "b68f961b-4cee-11e9-930a-005056a7f717",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cb20da45-4f6b-11e9-9a71-005056a7f717"
                    }
                },
                "uuid": "cb20da45-4f6b-11e9-9a71-005056a7f717",
            },
        }
    ),
]

```
</div>
</div>

---
### Retrieving a specific quota report record
This API is used to retrieve a specific quota report record. <br/>
The following example shows how to retrieve a single quota report user record.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = QuotaReport(
        index=281474976710656, **{"volume.uuid": "cf480c37-2a6b-11e9-8513-005056a7657c"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example1_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example1_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example1_result" class="try_it_out_content">
```
QuotaReport(
    {
        "index": 281474976710656,
        "qtree": {
            "name": "qt1",
            "id": 1,
            "_links": {
                "self": {
                    "href": "/api/storage/qtrees/cf480c37-2a6b-11e9-8513-005056a7657c/1"
                }
            },
        },
        "_links": {
            "self": {
                "href": "/api/storage/quota/reports/cf480c37-2a6b-11e9-8513-005056a7657c/281474976710656"
            }
        },
        "svm": {
            "name": "svm1",
            "_links": {
                "self": {"href": "/api/svm/svms/5093e722-248e-11e9-96ee-005056a7657c"}
            },
            "uuid": "5093e722-248e-11e9-96ee-005056a7657c",
        },
        "files": {
            "hard_limit": 40,
            "soft_limit": 30,
            "used": {"soft_limit_percent": 37, "hard_limit_percent": 28, "total": 11},
        },
        "space": {
            "hard_limit": 41943040,
            "soft_limit": 31457280,
            "used": {
                "soft_limit_percent": 34,
                "hard_limit_percent": 25,
                "total": 10567680,
            },
        },
        "volume": {
            "name": "fv",
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/cf480c37-2a6b-11e9-8513-005056a7657c"
                }
            },
            "uuid": "cf480c37-2a6b-11e9-8513-005056a7657c",
        },
        "type": "user",
        "users": [{"name": "fred", "id": "300008"}],
    }
)

```
</div>
</div>

---
### Retrieving a single quota report multi-user record
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = QuotaReport(
        index=281474976710656, **{"volume.uuid": "cf480c37-2a6b-11e9-8513-005056a7657c"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example2_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example2_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example2_result" class="try_it_out_content">
```
QuotaReport(
    {
        "index": 1153484454560268288,
        "qtree": {
            "name": "qt1",
            "id": 1,
            "_links": {
                "self": {
                    "href": "/api/storage/qtrees/cf480c37-2a6b-11e9-8513-005056a7657c/1"
                }
            },
        },
        "_links": {
            "self": {
                "href": "/api/storage/quota/reports/cf480c37-2a6b-11e9-8513-005056a7657c/1153484454560268288"
            }
        },
        "svm": {
            "name": "svm1",
            "_links": {
                "self": {"href": "/api/svm/svms/5093e722-248e-11e9-96ee-005056a7657c"}
            },
            "uuid": "5093e722-248e-11e9-96ee-005056a7657c",
        },
        "files": {
            "hard_limit": 40,
            "soft_limit": 30,
            "used": {"soft_limit_percent": 37, "hard_limit_percent": 28, "total": 11},
        },
        "space": {
            "hard_limit": 41943040,
            "soft_limit": 31457280,
            "used": {
                "soft_limit_percent": 34,
                "hard_limit_percent": 25,
                "total": 10567680,
            },
        },
        "volume": {
            "name": "fv",
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/cf480c37-2a6b-11e9-8513-005056a7657c"
                }
            },
            "uuid": "cf480c37-2a6b-11e9-8513-005056a7657c",
        },
        "type": "user",
        "users": [
            {"name": "fred", "id": "300008"},
            {"name": "john", "id": "300009"},
            {"name": "smith", "id": "300010"},
        ],
    }
)

```
</div>
</div>

---
### Retrieving a single quota report group record
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = QuotaReport(
        index=3459045988797251584,
        **{"volume.uuid": "cf480c37-2a6b-11e9-8513-005056a7657c"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example3_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example3_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example3_result" class="try_it_out_content">
```
QuotaReport(
    {
        "index": 3459045988797251584,
        "qtree": {
            "name": "qt1",
            "id": 1,
            "_links": {
                "self": {
                    "href": "/api/storage/qtrees/cf480c37-2a6b-11e9-8513-005056a7657c/1"
                }
            },
        },
        "_links": {
            "self": {
                "href": "/api/storage/quota/reports/cf480c37-2a6b-11e9-8513-005056a7657c/3459045988797251584"
            }
        },
        "svm": {
            "name": "svm1",
            "_links": {
                "self": {"href": "/api/svm/svms/5093e722-248e-11e9-96ee-005056a7657c"}
            },
            "uuid": "5093e722-248e-11e9-96ee-005056a7657c",
        },
        "files": {
            "hard_limit": 40,
            "soft_limit": 30,
            "used": {"soft_limit_percent": 37, "hard_limit_percent": 28, "total": 11},
        },
        "space": {
            "hard_limit": 41943040,
            "soft_limit": 31457280,
            "used": {
                "soft_limit_percent": 34,
                "hard_limit_percent": 25,
                "total": 10567680,
            },
        },
        "volume": {
            "name": "fv",
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/cf480c37-2a6b-11e9-8513-005056a7657c"
                }
            },
            "uuid": "cf480c37-2a6b-11e9-8513-005056a7657c",
        },
        "group": {"name": "test_group", "id": "500009"},
        "type": "group",
    }
)

```
</div>
</div>

---
### Retrieving a single quota report tree record
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    resource = QuotaReport(
        index=4612248968380809216,
        **{"volume.uuid": "cf480c37-2a6b-11e9-8513-005056a7657c"}
    )
    resource.get()
    print(resource)

```
<div class="try_it_out">
<input id="example4_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example4_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example4_result" class="try_it_out_content">
```
QuotaReport(
    {
        "index": 4612248968380809216,
        "qtree": {
            "name": "qt1",
            "id": 1,
            "_links": {
                "self": {
                    "href": "/api/storage/qtrees/cf480c37-2a6b-11e9-8513-005056a7657c/1"
                }
            },
        },
        "_links": {
            "self": {
                "href": "/api/storage/quota/reports/cf480c37-2a6b-11e9-8513-005056a7657c/4612248968380809216"
            }
        },
        "svm": {
            "name": "svm1",
            "_links": {
                "self": {"href": "/api/svm/svms/5093e722-248e-11e9-96ee-005056a7657c"}
            },
            "uuid": "5093e722-248e-11e9-96ee-005056a7657c",
        },
        "files": {
            "hard_limit": 40,
            "soft_limit": 30,
            "used": {"soft_limit_percent": 37, "hard_limit_percent": 28, "total": 11},
        },
        "space": {
            "hard_limit": 41943040,
            "soft_limit": 31457280,
            "used": {
                "soft_limit_percent": 34,
                "hard_limit_percent": 25,
                "total": 10567680,
            },
        },
        "volume": {
            "name": "fv",
            "_links": {
                "self": {
                    "href": "/api/storage/volumes/cf480c37-2a6b-11e9-8513-005056a7657c"
                }
            },
            "uuid": "cf480c37-2a6b-11e9-8513-005056a7657c",
        },
        "type": "tree",
    }
)

```
</div>
</div>

---
### Retrieving only records enforced by non-default rules
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(QuotaReport.get_collection(show_default_records=False)))

```
<div class="try_it_out">
<input id="example5_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example5_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example5_result" class="try_it_out_content">
```
[
    QuotaReport(
        {
            "index": 4612248968380809216,
            "qtree": {
                "name": "qt1",
                "id": 1,
                "_links": {
                    "self": {
                        "href": "/api/storage/qtrees/cf480c37-2a6b-11e9-8513-005056a7657c/1"
                    }
                },
            },
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cf480c37-2a6b-11e9-8513-005056a7657c/4612248968380809216"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/5093e722-248e-11e9-96ee-005056a7657c"
                    }
                },
                "uuid": "5093e722-248e-11e9-96ee-005056a7657c",
            },
            "files": {
                "hard_limit": 40,
                "soft_limit": 30,
                "used": {
                    "soft_limit_percent": 37,
                    "hard_limit_percent": 28,
                    "total": 11,
                },
            },
            "space": {
                "hard_limit": 41943040,
                "soft_limit": 31457280,
                "used": {
                    "soft_limit_percent": 34,
                    "hard_limit_percent": 25,
                    "total": 10567680,
                },
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cf480c37-2a6b-11e9-8513-005056a7657c"
                    }
                },
                "uuid": "cf480c37-2a6b-11e9-8513-005056a7657c",
            },
            "type": "tree",
        }
    ),
    QuotaReport(
        {
            "index": 1153484454560268288,
            "qtree": {
                "name": "qt1",
                "id": 1,
                "_links": {
                    "self": {
                        "href": "/api/storage/qtrees/cf480c37-2a6b-11e9-8513-005056a7657c/1"
                    }
                },
            },
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/cf480c37-2a6b-11e9-8513-005056a7657c/1153484454560268288"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/5093e722-248e-11e9-96ee-005056a7657c"
                    }
                },
                "uuid": "5093e722-248e-11e9-96ee-005056a7657c",
            },
            "files": {
                "hard_limit": 40,
                "soft_limit": 30,
                "used": {
                    "soft_limit_percent": 37,
                    "hard_limit_percent": 28,
                    "total": 11,
                },
            },
            "space": {
                "hard_limit": 41943040,
                "soft_limit": 31457280,
                "used": {
                    "soft_limit_percent": 34,
                    "hard_limit_percent": 25,
                    "total": 10567680,
                },
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/cf480c37-2a6b-11e9-8513-005056a7657c"
                    }
                },
                "uuid": "cf480c37-2a6b-11e9-8513-005056a7657c",
            },
            "type": "user",
            "users": [
                {"name": "fred", "id": "300008"},
                {"name": "john", "id": "300009"},
                {"name": "smith", "id": "300010"},
            ],
        }
    ),
]

```
</div>
</div>

---
### Retrieving quota report records with query parameters
The following example shows how to retrieve tree type quota report records.
<br/>
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(QuotaReport.get_collection(type="tree")))

```
<div class="try_it_out">
<input id="example6_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example6_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example6_result" class="try_it_out_content">
```
[
    QuotaReport(
        {
            "index": 2305843013508661248,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/8812b000-6e1e-11ea-9bad-00505682cd5c/2305843013508661248"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/903e54ee-6ccf-11ea-bc35-005056823577"
                    }
                },
                "uuid": "903e54ee-6ccf-11ea-bc35-005056823577",
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/8812b000-6e1e-11ea-9bad-00505682cd5c"
                    }
                },
                "uuid": "8812b000-6e1e-11ea-9bad-00505682cd5c",
            },
            "type": "tree",
        }
    ),
    QuotaReport(
        {
            "index": 2305843013508661248,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/a5ceebd2-6ccf-11ea-bc35-005056823577/2305843013508661248"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/903e54ee-6ccf-11ea-bc35-005056823577"
                    }
                },
                "uuid": "903e54ee-6ccf-11ea-bc35-005056823577",
            },
            "volume": {
                "name": "fg",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/a5ceebd2-6ccf-11ea-bc35-005056823577"
                    }
                },
                "uuid": "a5ceebd2-6ccf-11ea-bc35-005056823577",
            },
            "type": "tree",
        }
    ),
]

```
</div>
</div>

---
### Retrieving all the quota reports of a specific volume and the files fields
---
```python
from netapp_ontap import HostConnection
from netapp_ontap.resources import QuotaReport

with HostConnection("<mgmt-ip>", username="admin", password="password", verify=False):
    print(list(QuotaReport.get_collection(fields="files", **{"volume.name": "fv"})))

```
<div class="try_it_out">
<input id="example7_try_it_out" type="checkbox", class="try_it_out_check">
<label for="example7_try_it_out" class="try_it_out_button">Try it out</label>
<div id="example7_result" class="try_it_out_content">
```
[
    QuotaReport(
        {
            "index": 410328290557952,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/8812b000-6e1e-11ea-9bad-00505682cd5c/410328290557952"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/903e54ee-6ccf-11ea-bc35-005056823577"
                    }
                },
                "uuid": "903e54ee-6ccf-11ea-bc35-005056823577",
            },
            "files": {
                "hard_limit": 30,
                "soft_limit": 20,
                "used": {"soft_limit_percent": 0, "hard_limit_percent": 0, "total": 0},
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/8812b000-6e1e-11ea-9bad-00505682cd5c"
                    }
                },
                "uuid": "8812b000-6e1e-11ea-9bad-00505682cd5c",
            },
        }
    ),
    QuotaReport(
        {
            "index": 2305843013508661248,
            "_links": {
                "self": {
                    "href": "/api/storage/quota/reports/8812b000-6e1e-11ea-9bad-00505682cd5c/2305843013508661248"
                }
            },
            "svm": {
                "name": "svm1",
                "_links": {
                    "self": {
                        "href": "/api/svm/svms/903e54ee-6ccf-11ea-bc35-005056823577"
                    }
                },
                "uuid": "903e54ee-6ccf-11ea-bc35-005056823577",
            },
            "files": {
                "hard_limit": 400,
                "soft_limit": 200,
                "used": {"soft_limit_percent": 2, "hard_limit_percent": 1, "total": 4},
            },
            "volume": {
                "name": "fv",
                "_links": {
                    "self": {
                        "href": "/api/storage/volumes/8812b000-6e1e-11ea-9bad-00505682cd5c"
                    }
                },
                "uuid": "8812b000-6e1e-11ea-9bad-00505682cd5c",
            },
        }
    ),
]

```
</div>
</div>

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


__all__ = ["QuotaReport", "QuotaReportSchema"]
__pdoc__ = {
    "QuotaReportSchema.resource": False,
    "QuotaReportSchema.opts": False,
    "QuotaReport.quota_report_show": False,
    "QuotaReport.quota_report_create": False,
    "QuotaReport.quota_report_modify": False,
    "QuotaReport.quota_report_delete": False,
}


class QuotaReportSchema(ResourceSchema, metaclass=ResourceSchemaMeta):
    """The fields of the QuotaReport object"""

    links = fields.Nested("netapp_ontap.models.self_link.SelfLinkSchema", data_key="_links", unknown=EXCLUDE)
    r""" The links field of the quota_report."""

    files = fields.Nested("netapp_ontap.models.quota_report_files.QuotaReportFilesSchema", data_key="files", unknown=EXCLUDE)
    r""" The files field of the quota_report."""

    group = fields.Nested("netapp_ontap.models.quota_report_group.QuotaReportGroupSchema", data_key="group", unknown=EXCLUDE)
    r""" The group field of the quota_report."""

    index = Size(
        data_key="index",
    )
    r""" Index that identifies a unique quota record. Valid in URL."""

    qtree = fields.Nested("netapp_ontap.models.quota_report_qtree.QuotaReportQtreeSchema", data_key="qtree", unknown=EXCLUDE)
    r""" The qtree field of the quota_report."""

    space = fields.Nested("netapp_ontap.models.quota_report_space.QuotaReportSpaceSchema", data_key="space", unknown=EXCLUDE)
    r""" The space field of the quota_report."""

    specifier = fields.Str(
        data_key="specifier",
    )
    r""" Quota specifier"""

    svm = fields.Nested("netapp_ontap.resources.svm.SvmSchema", data_key="svm", unknown=EXCLUDE)
    r""" The svm field of the quota_report."""

    type = fields.Str(
        data_key="type",
        validate=enum_validation(['tree', 'user', 'group']),
    )
    r""" Quota type associated with the quota record.

Valid choices:

* tree
* user
* group"""

    users = fields.List(fields.Nested("netapp_ontap.models.quota_report_users.QuotaReportUsersSchema", unknown=EXCLUDE), data_key="users")
    r""" This parameter specifies the target user or users associated with the given quota report record. This parameter is available for user quota records and is not available for group or tree quota records. The target user or users are identified by a user name and user identifier. The user name can be a UNIX user name or a Windows user name, and the identifer can be a UNIX user identifier or a Windows security identifier."""

    volume = fields.Nested("netapp_ontap.resources.volume.VolumeSchema", data_key="volume", unknown=EXCLUDE)
    r""" The volume field of the quota_report."""

    @property
    def resource(self):
        return QuotaReport

    gettable_fields = [
        "links",
        "files",
        "group",
        "index",
        "qtree.links",
        "qtree.id",
        "qtree.name",
        "space",
        "specifier",
        "svm.links",
        "svm.name",
        "svm.uuid",
        "type",
        "users",
        "volume.links",
        "volume.name",
        "volume.uuid",
    ]
    """links,files,group,index,qtree.links,qtree.id,qtree.name,space,specifier,svm.links,svm.name,svm.uuid,type,users,volume.links,volume.name,volume.uuid,"""

    patchable_fields = [
        "files",
        "group",
        "qtree.id",
        "qtree.name",
        "space",
        "svm.name",
        "svm.uuid",
        "users",
        "volume.name",
        "volume.uuid",
    ]
    """files,group,qtree.id,qtree.name,space,svm.name,svm.uuid,users,volume.name,volume.uuid,"""

    postable_fields = [
        "files",
        "group",
        "qtree.id",
        "qtree.name",
        "space",
        "svm.name",
        "svm.uuid",
        "users",
        "volume.name",
        "volume.uuid",
    ]
    """files,group,qtree.id,qtree.name,space,svm.name,svm.uuid,users,volume.name,volume.uuid,"""

def _get_field_list(field: str) -> Callable[[], List]:
    def getter():
        return [getattr(r, field) for r in QuotaReport.get_collection(fields=field)]
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
            raise NetAppRestError("QuotaReport modify job failed")
        if job.state == "success":
            break
        await asyncio.sleep(1)

class QuotaReport(Resource):
    """Allows interaction with QuotaReport objects on the host"""

    _schema = QuotaReportSchema
    _path = "/api/storage/quota/reports"
    _keys = ["volume.uuid", "index"]

    @classmethod
    def get_collection(
        cls,
        *args,
        connection: HostConnection = None,
        max_records: int = None,
        **kwargs
    ) -> Iterable["Resource"]:
        r"""Retrieves the quota report records for all FlexVol volumes and FlexGroup volumes.
### Related ONTAP commands
* `quota report`

### Learn more
* [`DOC /storage/quota/reports`](#docs-storage-storage_quota_reports)"""
        return super()._get_collection(*args, connection=connection, max_records=max_records, **kwargs)

    get_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get_collection.__doc__)

    if RECLINE_INSTALLED:
        @recline.command(name="quota report show")
        def quota_report_show(
            fields: List[Choices.define(["index", "specifier", "type", "*"])]=None,
        ) -> ResourceTable:
            """Fetch a list of QuotaReport resources

            Args:
                index: Index that identifies a unique quota record. Valid in URL.
                specifier: Quota specifier
                type: Quota type associated with the quota record.
            """

            kwargs = {}
            if index is not None:
                kwargs["index"] = index
            if specifier is not None:
                kwargs["specifier"] = specifier
            if type is not None:
                kwargs["type"] = type
            if fields is not None:
                fields = ",".join(fields)
                kwargs["fields"] = fields

            return QuotaReport.get_collection(
                **kwargs
            )

    @classmethod
    def count_collection(
        cls,
        *args,
        connection: HostConnection = None,
        **kwargs
    ) -> int:
        """Returns a count of all QuotaReport resources that match the provided query"""
        return super()._count_collection(*args, connection=connection, **kwargs)

    count_collection.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._count_collection.__doc__)




    @classmethod
    def find(cls, *args, connection: HostConnection = None, **kwargs) -> Resource:
        r"""Retrieves the quota report records for all FlexVol volumes and FlexGroup volumes.
### Related ONTAP commands
* `quota report`

### Learn more
* [`DOC /storage/quota/reports`](#docs-storage-storage_quota_reports)"""
        return super()._find(*args, connection=connection, **kwargs)

    find.__func__.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._find.__doc__)

    def get(self, **kwargs) -> NetAppResponse:
        r"""Retrieves a specific quota report record.
### Related ONTAP commands
* `quota report`

### Learn more
* [`DOC /storage/quota/reports`](#docs-storage-storage_quota_reports)"""
        return super()._get(**kwargs)

    get.__doc__ += "\n\n---\n" + inspect.cleandoc(Resource._get.__doc__)





