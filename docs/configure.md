You can configure the Delta instance to output different data formats by setting the `dtype` attribute of the configuration object. The default format is `json`, but you can change it to `polars` for better performance or other formats as needed.

```python
from deltabase import delta

db:delta = delta.connect(path="local_path/mydelta")
db.config.dtype = "polars"
```

---
