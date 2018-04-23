package neo4j

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

var testDB bolt.Conn

func TestMain(m *testing.M) {
	rand.Seed(time.Now().Unix())
	var err error
	testDB, err = NewConn()
	if err != nil {
		panic(err)
	}
	defer testDB.Close()
	log.Println("connected")
	code := m.Run()
	os.Exit(code)
}

// func TestResetPageRank(t *testing.T) {
// 	err := ResetPageRankQuery(testDB)
// 	assert.Nil(t, err)
// }

// func TestRankAlgo(t *testing.T) {
// 	err := RankAlgo(testDB)
// 	assert.Nil(t, err)
// }

// func TestTopRanked(t *testing.T) {
// 	_, err := TopRanked(testDB)
// 	assert.Nil(t, err)
// }

func randNum() int64 {
	return rand.Int63n(500000)
}

func BenchmarkDepth1(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth1, r)
		_, err := ExecQuery(query, testDB)
		if err != nil {
			panic(err)
		}
		// rows, _ := RunQuery(query, testDB)
		// rows.Close()
	}
}

func BenchmarkDepth2(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth2, r)
		_, err := ExecQuery(query, testDB)
		if err != nil {
			panic(err)
		}
		// rows, _ := RunQuery(query, testDB)
		// rows.Close()
	}
}

func BenchmarkDepth3(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth3, r)
		_, err := ExecQuery(query, testDB)
		if err != nil {
			panic(err)
		}
		// rows, _ := RunQuery(query, testDB)
		// rows.Close()
	}
}

func BenchmarkDepth4(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth4, r)
		_, err := ExecQuery(query, testDB)
		if err != nil {
			panic(err)
		}
		// rows, _ := RunQuery(query, testDB)
		// rows.Close()
	}
}

func BenchmarkDepth5(b *testing.B) {
	b.N = 20
	for i := 0; i < b.N; i++ {
		r := randNum()
		query := fmt.Sprintf(endorsementDepth5, r)
		_, err := ExecQuery(query, testDB)
		if err != nil {
			panic(err)
		}
		// rows, _ := RunQuery(query, testDB)
		// rows.Close()
	}
}
