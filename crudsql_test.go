package crudsql_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/Richtermnd/crudsql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

func suite(t *testing.T) (db *sqlx.DB, crud *crudsql.CRUD[Person], records map[int64]Person) {
	db, _ = sqlx.Connect("sqlite3", t.Name())
	t.Cleanup(func() {
		db.Close()
		os.Remove(t.Name())
	})

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

	return db, crudsql.New[Person](db, crudsql.Question), records
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
	_, crud, records := suite(t)

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

func TestReadNotEixistent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, crud, _ := suite(t)

	_, err := crud.Get(ctx, 123456789)
	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	if err != sql.ErrNoRows {
		t.Fatalf("Expected %v, got %v", sql.ErrNoRows, err)
	}
}

func TestReadAll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, crud, records := suite(t)

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
	db, crud, records := suite(t)

	id := records[1].ID
	err := crud.Delete(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow("SELECT * FROM persons WHERE id = ?", id)
	p := Person{}
	err = row.Scan(&p.ID, &p.Name)
	if err == nil {
		t.Fatal("Item not deleted")
	}
	if err != sql.ErrNoRows {
		t.Fatalf("Expected %v, got %v", sql.ErrNoRows, err)
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, crud, _ := suite(t)

	err := crud.Update(ctx, 1, Person{Name: "foo"})
	if err != nil {
		t.Fatal(err)
	}
	var item Person
	db.Get(&item, "SELECT * FROM persons WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	if item.Name != "foo" {
		t.Fatalf("Expected %v, got %v", "foo", item.Name)
	}
}
