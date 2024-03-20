package crudsql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Richtermnd/crudsql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var db *sqlx.DB
var records map[int64]Person

func init() {
	db, _ = sqlx.Connect("sqlite3", "test.db")

	db.MustExec("DROP TABLE IF EXISTS persons")
	db.MustExec("CREATE TABLE IF NOT EXISTS persons (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	db.MustExec("INSERT INTO persons (name) VALUES ('Joe')")
	db.MustExec("INSERT INTO persons (name) VALUES ('Mary')")
	db.MustExec("INSERT INTO persons (name) VALUES ('John')")

	records = make(map[int64]Person)
	var id int64
	var name string
	rows, err := db.Query("SELECT id, name FROM persons")
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	for rows.Next() {
		rows.Scan(&id, &name)
		records[id] = Person{ID: id, Name: name}
	}
}

// SQLRecord example
type Person struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func (p Person) Table() string {
	return "persons"
}

func (p Person) Columns() []string {
	return []string{"id", "name"}
}

func (p Person) Map() map[string]interface{} {
	return map[string]interface{}{
		"id":   p.ID,
		"name": p.Name,
	}
}

func (p Person) PrimaryKey() (key string, value interface{}) {
	return "id", p.ID
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	crud := crudsql.New[Person](db)

	for id, person := range records {
		item, err := crud.Read(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if item.ID != person.ID || item.Name != person.Name {
			t.Fatalf("Expected %v, got %v", person, item)
		}
	}
}

func TestReadAll(t *testing.T) {
	ctx := context.Background()
	crud := crudsql.New[Person](db)
	items, err := crud.ReadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(records) {
		t.Fatalf("Expected %v, got %v", len(records), len(items))
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	crud := crudsql.New[Person](db)

	// insert new record and delete it
	res, err := db.Exec("Insert INTO persons (name) VALUES ('foo')")
	if err != nil {
		t.Fatal(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	err = crud.Delete(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	crud := crudsql.New[Person](db)
	t.Log(crud.Read(ctx, 1))
	err := crud.Update(ctx, 1, Person{Name: "foo"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := crud.Read(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if item.Name != "foo" {
		t.Fatalf("Expected %v, got %v", "foo", item.Name)
	}
}
