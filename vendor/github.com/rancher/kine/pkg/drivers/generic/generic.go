package generic

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	revSQL  = `
		SELECT rkv.id
		FROM key_value rkv
		ORDER BY rkv.id
		DESC LIMIT 1`

	compactRevSQL = `
		SELECT crkv.prev_revision
		FROM key_value crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC LIMIT 1`

	idOfKey = `
		AND mkv.id <= ? AND mkv.id > (
			SELECT ikv.id
			FROM key_value ikv
			WHERE
				ikv.name = ? AND
				ikv.id <= ?
			ORDER BY ikv.id DESC LIMIT 1)`

	listSQL = fmt.Sprintf(`SELECT (%s), (%s), %s
		FROM key_value kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM key_value mkv
			WHERE
				mkv.name LIKE ?
				%%s
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR ?)
		ORDER BY kv.id ASC
		`, revSQL, compactRevSQL, columns)
)

type Stripped string

func (s Stripped) String() string {
	str := strings.ReplaceAll(string(s), "\n", "")
	return regexp.MustCompile("[\t ]+").ReplaceAllString(str, " ")
}

type Generic struct {
	mutex                 sync.Mutex
	LastInsertID          bool
	DB                    *sql.DB
	GetCurrentSQL         string
	GetRevisionSQL        string
	RevisionSQL           string
	ListRevisionStartSQL  string
	GetRevisionAfterSQL   string
	CountSQL              string
	AfterSQL              string
	DeleteSQL             string
	UpdateCompactSQL      string
	InsertSQL             string
	InsertLastInsertIDSQL string
}

func q(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func Open(driverName, dataSourceName string, paramCharacter string, numbered bool) (*Generic, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	return &Generic{
		DB: db,

		GetRevisionSQL: q(fmt.Sprintf(`
			SELECT
			0, 0, %s
			FROM key_value kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  q(fmt.Sprintf(listSQL, idOfKey), paramCharacter, numbered),

		CountSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		AfterSQL: q(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM key_value kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, columns), paramCharacter, numbered),

		DeleteSQL: q(`
			DELETE FROM key_value
			WHERE id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE key_value
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO key_value(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO key_value(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, paramCharacter, numbered),
	}, nil
}

func (d *Generic) query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	logrus.Tracef("QUERY %v : %s", args, Stripped(sql))
	return d.DB.QueryContext(ctx, sql, args...)
}

func (d *Generic) queryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	logrus.Tracef("QUERY ROW %v : %s", args, Stripped(sql))
	return d.DB.QueryRowContext(ctx, sql, args...)
}

func (d *Generic) execute(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	logrus.Tracef("EXEC %v : %s", args, Stripped(sql))
	return d.DB.ExecContext(ctx, sql, args...)
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, compactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	result, err := d.execute(ctx, d.UpdateCompactSQL, revision)
	if err != nil {
		return err
	}
	num, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if num != 0 {
		return nil
	}
	_, err = d.Insert(ctx, "compact_rev_key", false, false, 0, revision, 0, []byte(""), nil)
	return err
}

func (d *Generic) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, d.GetRevisionSQL, revision)
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, sql, prefix, revision, startKey, revision, includeDeleted)
}

func (d *Generic) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, d.CountSQL, prefix, false)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) After(ctx context.Context, prefix string, rev int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	return d.query(ctx, sql, prefix, rev)
}

func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 00, err
		}
		return row.LastInsertId()
	}

	row := d.queryRow(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
	err = row.Scan(&id)
	return id, err
}
