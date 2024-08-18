To connect to a delta source instance, use the `connect` method provided by the `delta` class. This method allows you to connect to either a local file path or a remote cloud storage service (such as AWS S3, Azure Data Lake, or Google Cloud Storage). The method returns an instance of the `delta` class, which you can use to manage your Delta tables.

```python 
from deltabase import delta

db:delta = delta.connect(path="local_path/mydelta")
```

---

> on connect, delta will attempt to load tables if path is local.

---

> `#!python db:delta = delta.connect(path="s3://<bucket>")`

You can also connect to a delta source stored in an AWS S3 bucket. Replace `<bucket>` with the actual bucket name and path.

---

> `#!python db:delta = delta.connect(path="<az|adl|abfs[s]>://<container>")`

For Azure, you can connect to an Azure Data Lake Storage (ADLS) or Azure Blob Storage using the appropriate URI scheme (`az://`, `adl://`, or `abfs[s]://`).

---

> `#!python db:delta = delta.connect(path="gs://<bucket>")`

For Google Cloud Storage, use the `gs://` prefix followed by the bucket name and path.

---
