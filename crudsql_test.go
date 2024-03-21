package crudsql_test

import (
	"context"
	"os"
	"testing"

	"github.com/Richtermnd/crudsql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

func suite(testName string) (crud *crudsql.CRUD[Person], records map[int64]Person, cleanup func()) {
	db, _ := sqlx.Connect("sqlite3", testName)
	cleanup = func() {
		db.Close()
		os.Remove(testName)
	}

	names := [5]string{
		"Joe",
		"Mary",
		"John",
		"Jane",
		"Bob",
	}

	records = make(map[int64]Person)
	db.MustExec("DROP TABLE IF EXISTS persons")
	db.MustExec("CREATE TABLE IF NOT EXISTS persons (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	for _, name := range names {
		res := db.MustExec("INSERT INTO persons (name) VALUES (?)", name)
		id, _ := res.LastInsertId()
		records[id] = Person{
			ID:   id,
			Name: name,
		}
	}

	return crudsql.New[Person](db, crudsql.Question), records, cleanup
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
	t.Parallel()
	ctx := context.Background()
	crud, records, cleanup := suite(t.Name())
	t.Cleanup(cleanup)

	for id, person := range records {
		item, err := crud.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if item.ID != person.ID || item.Name != person.Name {
			t.Fatalf("Expected %v, got %v", person, item)
		}
	}
}

func TestReadAll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	crud, records, cleanup := suite(t.Name())
	t.Cleanup(cleanup)

	items, err := crud.GetAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != len(records) {
		t.Fatalf("Expected %v, got %v", len(records), len(items))
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	crud, records, cleanup := suite(t.Name())
	t.Cleanup(cleanup)

	id := records[1].ID
	err := crud.Delete(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	_, err = crud.Get(ctx, id)
	if err == nil {
		t.Fatal("Expected error")
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	crud, _, cleanup := suite(t.Name())
	t.Cleanup(cleanup)

	err := crud.Update(ctx, 1, Person{Name: "foo"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := crud.Get(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if item.Name != "foo" {
		t.Fatalf("Expected %v, got %v", "foo", item.Name)
	}
}
