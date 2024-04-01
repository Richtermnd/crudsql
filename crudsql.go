package crudsql

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

// Aliases for squirrel placeholders
var (
	// Question is a PlaceholderFormat instance that leaves placeholders as
	// question marks.
	Question = sq.Question // sqlite

	// Dollar is a PlaceholderFormat instance that replaces placeholders with
	// dollar-prefixed positional placeholders (e.g. $1, $2, $3).
	Dollar = sq.Dollar // Postgres

	// Colon is a PlaceholderFormat instance that replaces placeholders with
	// colon-prefixed positional placeholders (e.g. :1, :2, :3).
	Colon = sq.Colon

	// AtP is a PlaceholderFormat instance that replaces placeholders with
	// "@p"-prefixed positional placeholders (e.g. @p1, @p2, @p3).
	AtP = sq.AtP
)

type SQLRecord interface {
	// Name of table
	Table() string

	// Columns in table
	Columns() []string

	// Struct as map[string]interface{}
	Map() map[string]interface{}

	// Primary key and value for this record
	PrimaryKey() (key string, value interface{})
}

type CRUD[T SQLRecord] struct {
	db *sqlx.DB
	sq sq.StatementBuilderType
}

func New[T SQLRecord](db *sqlx.DB, placeholder sq.PlaceholderFormat) *CRUD[T] {
	return &CRUD[T]{
		db: db,
		sq: sq.StatementBuilder.PlaceholderFormat(placeholder),
	}
}

func (c *CRUD[T]) Create(ctx context.Context, item T) error {
	m := item.Map()
	cols := item.Columns()

	// get model values
	values := make([]interface{}, len(m))
	for _, v := range cols {
		values = append(values, m[v])
	}

	// build query
	stmt, args, err := c.sq.
		Insert(item.Table()).
		Columns(item.Columns()...).
		Values(values...).
		ToSql()
	if err != nil {
		return err
	}
	// exec query
	_, err = c.db.ExecContext(ctx, stmt, args...)
	return err
}

func (c *CRUD[T]) Get(ctx context.Context, pk interface{}) (item T, err error) {
	pkColumn, _ := item.PrimaryKey()
	stmt, args, err := c.sq.
		Select("*").
		From(item.Table()).
		Where(sq.Eq{pkColumn: pk}).
		ToSql()
	if err != nil {
		return
	}
	err = c.db.GetContext(ctx, &item, stmt, args...)
	return
}

func (c *CRUD[T]) GetAll(ctx context.Context) (items []T, err error) {
	var temp T
	stmt, args, err := c.sq.Select("*").From(temp.Table()).ToSql()
	if err != nil {
		return
	}
	err = c.db.SelectContext(ctx, &items, stmt, args...)
	return
}

func (c *CRUD[T]) Update(ctx context.Context, pk interface{}, item T) error {
	pkColumn, _ := item.PrimaryKey()
	recordMap := item.Map()
	delete(recordMap, pkColumn)

	stmt, args, err := c.sq.
		Update(item.Table()).
		SetMap(recordMap).
		Where(sq.Eq{pkColumn: pk}).
		ToSql()
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, stmt, args...)
	return err
}

func (c *CRUD[T]) Delete(ctx context.Context, pk interface{}) error {
	var temp T
	pkColumn, _ := temp.PrimaryKey()
	stmt, args, err := c.sq.
		Delete(temp.Table()).
		Where(sq.Eq{pkColumn: pk}).
		ToSql()
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, stmt, args...)
	return err
}
