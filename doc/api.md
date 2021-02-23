# API description and generation  

## DATABASE PROCEDURE INTERFACE

### how I can generate database API in golang code?

In first, we must describe the structures of the database using yaml syntax
```yaml
schemas:
  - name: schema_name    # database schema name
    tables:
      some_table:        # table name
        columns:         # array of columns
          - name: id
            schema: {type: int8, not_null: true}
            constraints:
              - type: primary key
          - name: name
            schema: {type: varchar, length: 20, not_null: true}
          - name: volume
            schema: {type: decimal, length: 12, precision: 2}
        api:             # needed database APIs
          - name: insertSomeTableEntity
            type: insertOne
          - name: updateSomeTableEntity
            type: updateOne
          - name: deleteSomeTableEntity
            type: deleteOne
          - name: lookupSomeTableEntity
            type: lookUp
```
the `api` section describes the types of procedures that need to be generated
consider the types that already exist:
 - `insertOne`
 - `upsertOne`
 - `updateOne`
 - `updateAll`
 - `deleteOne`
 - `deleteAll`
 - `findOne`
 - `findAll`
 - `findAllPaginate`
 - `lookUp`
 
each of these types has a meaningful name — its name describes the work being done. For interfaces that contain the word `One` in the name, the data entered for the search guarantees the match of only one record, for this a primary key or a unique index is used. For interfaces in the name of which the word `All` is used, the data list for search is configured using the "find_by" section.

The specified list can be extended using the dragonfly.RegisterApiBuilder procedural interface.

### how to prepare data structures for searching records

in order to configure the set of fields for the search structure used in multi-line procedures (such as updateAll, deleteAll, findAll, findAllPaginate), you can list the search options in the `find_by` section
 - `equal`
 - `notEqual`
 - `like`
 - `notLike`
 - `in`
 - `notIn`
 - `great`
 - `less`
 - `notGreat`
 - `notLess`
 - `starts`
 - `isNull`

```yaml
api:
  - name: updateSomeTableEntities
    type: updateAll
    find_by:
      - column: name
        operator: in
      - column: volume
        operator: great
```
this example will be generated into the following code:
```go
type UpdateSomeTableEntitiesOption struct {
	Name   []string `sql:"name"`
	Volume *float64 `sql:"volume,omitempty"`
}

func UpdateSomeTableEntities(ctx context.Context, filter UpdateSomeTableEntitiesOption) (result []SchemaNameSomeTableRow, err error) {
    var (
        sqlText = "update schema_name.some_table set %s where %s returning id, name, volume"
        ...
    )
    ...
    var arrayName []string
    for _, opt := range filter.Name {
        args = append(args, opt)
        arrayName = append(arrayName, "$"+strconv.Itoa(len(args)))
    }
    if len(arrayName) > 0 {
        filters = append(filters, fmt.Sprintf("%s in (%s)", "name", strings.Join(arrayName, ", ")))
    }
    if filter.Volume != nil {
        args = append(args, *filter.Volume)
        filters = append(filters, fmt.Sprintf("%s > %s", "volume", "$"+strconv.Itoa(len(args))))
    }
    ...
    rows, err = db.Query(sqlText, args...)
    ...
}
```

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