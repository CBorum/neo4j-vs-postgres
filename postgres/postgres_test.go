package postgres

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const testDBURL = "postgres://postgres@localhost/?sslmode=disable"

var testDB dbi

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	var err error
	testDB, err = NewDB(testDBURL)
	if err != nil {
		panic(err)
	}
	defer testDB.Close()
	code := m.Run()
	os.Exit(code)
}

func randNum() int64 {
	return rand.Int63n(500000)
}

func TestCreateDataTables(t *testing.T) {
	err := testDB.Ping()
	assert.Nil(t, err)

	_, err = testDB.Query(`
		CREATE TABLE person (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255),
			job VARCHAR(255),
			birthday VARCHAR(255)
		);
	`)
	assert.Nil(t, err)

	_, err = testDB.Query(`
		CREATE TABLE relation (
			id1 INTEGER REFERENCES person(id),
			id2 INTEGER REFERENCES person(id)
		);
	`)
	assert.Nil(t, err)

	//create index on ids

	fmt.Println("success")
}

func TestInsertData(t *testing.T) {
	err := testDB.Ping()
	assert.Nil(t, err)

	_, err = testDB.Query(`
		COPY person FROM PROGRAM 'tail -n +2 /import/social_network_nodes.csv'
		WITH (FORMAT csv);
	`)
	assert.Nil(t, err)

	_, err = testDB.Query(`
		COPY relation FROM PROGRAM 'tail -n +2 /import/social_network_edges.csv'
		WITH (FORMAT csv);
	`)
	assert.Nil(t, err)

	fmt.Println("success")
}

func BenchmarkDepth1(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth1, r)
		rows, _ := testDB.Query(query)
		rows.Close()
	}
}

func BenchmarkDepth2(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth2, r)
		rows, _ := testDB.Query(query)
		rows.Close()
	}
}

func BenchmarkDepth3(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth3, r)
		rows, _ := testDB.Query(query)
		rows.Close()
	}
}

func BenchmarkDepth4(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth4, r)
		rows, _ := testDB.Query(query)
		rows.Close()
	}
}

func BenchmarkDepth5(b *testing.B) {
	b.N = 1
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth5, r)
		rows, _ := testDB.Query(query)
		rows.Close()
	}
}
