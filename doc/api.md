# API description and generation  


## AUTOGENERATION  

### how to generate datetime value

put `generate(now)` into column tags, like this:

```yaml
schemas:
  - name: test
    tables:
      test_table:
        columns:
          - name: uuid
            schema: { type: uuid }
            tags: [ generate(uuid) ]
          - name: someOther
            schema: { type: varchar, length: 20 }
          - name: created
            schema: { type: timestamp }
            tags: [ generate(now) ]  # HERE
```

and generator will make something like this:

```go
    ...
	fields = append(fields, "created")
	args = append(args, time.Now())
	values = append(values, fmt.Sprintf("/* created */ $%d", len(args)))
    ...
```

### how can i use this?

let's make timestamp `delete flag` column:

```yaml
schemas:
  - name: test
    tables:
      test_table:
        columns:
          - name: uuid
            schema: { type: uuid }
            tags: [ generate(uuid) ]
          - name: someOther
            schema: { type: varchar, length: 20 }
          - name: created
            schema: { type: timestamp }
            tags: [ noInsert, generate(now) ]
          - name: deleted
            schema: { type: timestamp }
            tags: [ noInsert, noUpdate, generate(now) ]  # HERE

        api:
          - type: insertOne
          - type: updateOne
          - name: deleteRecord
            type: updateOne
            modify: [ deleted ] # AND HERE
          - name: findRecords
            type: findAll
            find_by:
              - column: someOther
              - column: deleted
                operator: isNull
                constant: "null" # AND HERE
```

Look, I set the following tags for the `deleted` column: noInsert and noUpdate.
Now the generator will omit this column when implicitly generating a list of mutable data.
But, I indicate explicitly that for the API `name: deleteRecord` the only variable column is `deleted`, so the generator has to take it into account, but it is not in the list of options, because generated automatically.
Now it turns out that the `deleteRecord` function will not delete the records for real, but will mark them with the delete time.

Now pay attention to the additional option `constant` of the column `deleted` in the list `find_by`.
This means that the data for this option will not be requested from the user, but will always have a fixed value.
Thus, the `findRecords` function will always return data in which the `deleted` column is Null.


### how to generate uuid value for column

1. make generator function  


```go
package generated

import (
	"crypto/rand"
	"fmt"
)

func uuidGenerate() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // never happens
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
```

2. register it  


```go
    ...
    dragonfly.RegisterFieldValueGenerator("uuid", "uuidGenerate", 0, false)
    ...
```

where  

 - `uuid` - function alias used as tag in yaml files
 - `uuidGenerate` - real function name
 - `0` - minimal count of function arguments
 - `false` - if you specify "true" here, then the number of function arguments will be considered extensible
 
 3. use it in yaml files
 
 ```yaml
schemas:
  - name: test
    tables:
      test_table:
        columns:
          - name: uuid
            schema: { type: uuid }
            tags: [ generate(uuid) ] # HERE
          - name: created
            schema: { type: timestamp }
            tags: [ generate(now) ]
          - name: someOther
            schema: { type: varchar, length: 20 }

        api:
          - type: insertOne
          - type: lookUp
```

and generator will make something like this:

```go
    ...
    fields = append(fields, "uuid")
    args = append(args, uuidGenerate())
    values = append(values, fmt.Sprintf("/* uuid */ $%d", len(args)))
    ...
```

## STRUCTURES  

### how do I hide some fields?

How can we isolate some fields inside the database so that the GO code does not know anything about them?    
Just specify the `ignore` tag for these fields.

```yaml
    ...
  some_table:
    columns:
      - name: hidden_field
        schema: { type: varchar, length: 36 }
        tags: [ ignore ]  # HERE
    ...
```