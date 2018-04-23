package postgres

import (
	"database/sql"

	_ "github.com/lib/pq"
)

const endorsementDepth1 = `
SELECT name,id FROM person
WHERE id IN (
	SELECT id2 FROM relation
	WHERE relation.id1 IN (
		SELECT id FROM person LIMIT 1 OFFSET %d
	)
);
`

const endorsementDepth2 = `
SELECT name,id FROM person
WHERE id IN (
	SELECT id2 FROM relation
	WHERE relation.id1 IN (
		SELECT id FROM person
		WHERE id IN (
			SELECT id2 FROM relation
			WHERE relation.id1 IN (
				SELECT id FROM person LIMIT 1 OFFSET %d
			)
		)
	)
);
`

const endorsementDepth3 = `
SELECT name,id FROM person
WHERE id IN (
	SELECT id2 FROM relation
	WHERE relation.id1 IN (
		SELECT id FROM person
		WHERE id IN (
			SELECT id2 FROM relation
			WHERE relation.id1 IN (
				SELECT id FROM person
				WHERE id IN (
					SELECT id2 FROM relation
					WHERE relation.id1 IN (
						SELECT id FROM person LIMIT 1 OFFSET %d
					)
				)
			)
		)
	)
);
`

const endorsementDepth4 = `
SELECT name,id FROM person
WHERE id IN (
	SELECT id2 FROM relation
	WHERE relation.id1 IN (
		SELECT id FROM person
		WHERE id IN (
			SELECT id2 FROM relation
			WHERE relation.id1 IN (
				SELECT id FROM person
				WHERE id IN (
					SELECT id2 FROM relation
					WHERE relation.id1 IN (
						SELECT id FROM person
						WHERE id IN (
							SELECT id2 FROM relation
							WHERE relation.id1 IN (
								SELECT id FROM person LIMIT 1 OFFSET %d
							)
						)
					)
				)
			)
		)
	)
);
`

const endorsementDepth5 = `
SELECT name,id FROM person
WHERE id IN (
	SELECT id2 FROM relation
	WHERE relation.id1 IN (
		SELECT id FROM person
		WHERE id IN (
			SELECT id2 FROM relation
			WHERE relation.id1 IN (
				SELECT id FROM person
				WHERE id IN (
					SELECT id2 FROM relation
					WHERE relation.id1 IN (
						SELECT id FROM person
						WHERE id IN (
							SELECT id2 FROM relation
							WHERE relation.id1 IN (
								SELECT id FROM person
								WHERE id IN (
									SELECT id2 FROM relation
									WHERE relation.id1 IN (
										SELECT id FROM person LIMIT 1 OFFSET %d
									)
								)
							)
						)
					)
				)
			)
		)
	)
);
`

type dbi interface {
	Query(string, ...interface{}) (*sql.Rows, error)
	Ping() error
	Close() error
}

func NewDB(dataSourceName string) (dbi, error) {
	return sql.Open("postgres", dataSourceName)
}

func RunQuery(db dbi, query string) (*sql.Rows, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
