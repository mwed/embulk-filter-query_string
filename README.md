# Query String filter plugin for Embulk

The query string filter plugin parses the column contents as query string and insert columns from that field.

## Overview

* **Plugin type**: filter

## Configuration

- **query_string_column_name**:  (string, required)
- **expanded_columns**: columns expanded into multiple columns (array, required)
  - **name**: name of the fields.
  - **type**: type of the column.

## Example

```yaml
type: query_string
query_string_column_name: qs
expanded_columns:
  - {name: f1, type: string}
  - {name: f2, type: long}
  - {name: f3, type: string}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## License

```
Copyright 2016 Minnano Wedding Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
