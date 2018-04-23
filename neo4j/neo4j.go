package neo4j

import (
	"math"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

// const setPageRankQuery = `
// MATCH (a:Person)
// SET a.pageRank = 1.0
// `

// const recursiveRankAlgo = `
// MATCH (a:Person)
// WITH
//   collect(DISTINCT a) AS pages
// UNWIND pages as dest
//   MATCH (source:Person)-[:ENDORSES]->(dest:Person)
//   WITH
//     collect(DISTINCT source) AS sources,
//     dest AS dest
//     UNWIND sources AS src
//       MATCH (src:Person)-[r:ENDORSES]->(:Person)
//       WITH
//         src.pageRank / count(r) AS points,
//         dest AS dest
//       WITH
//         sum(points) AS p,
//         dest AS dest
//       SET dest.pageRank = 0.15 + 0.85 * p;
// `

// const topRankedQuery = `
// MATCH (a:Person)
// RETURN id(a), a.name, a.pageRank
// ORDER BY a.pageRank DESC
// LIMIT 25;
// `

const endorsementDepth1 = `
MATCH (p:Person)
WITH p SKIP %d LIMIT 1
MATCH (:Person {name: p.name})-[:ENDORSES]->(a:Person)
RETURN DISTINCT a
`

// const endorsementDepth1 = `
// MATCH (:Person {name: "Jeanie Mountcastle"})-[:ENDORSES]->(a:Person)
// RETURN DISTINCT a

const endorsementDepth2 = `
MATCH (p:Person)
WITH p SKIP %d LIMIT 1
MATCH (:Person {name: p.name})-[:ENDORSES]->(:Person)-[:ENDORSES]->(a:Person)
RETURN DISTINCT a
`

const endorsementDepth3 = `
MATCH (p:Person)
WITH p SKIP %d LIMIT 1
MATCH (:Person {name: p.name})-[:ENDORSES]->(:Person)-[:ENDORSES]->(:Person)-[:ENDORSES]->(a:Person)
RETURN DISTINCT a
`

const endorsementDepth4 = `
MATCH (p:Person)
WITH p SKIP %d LIMIT 1
MATCH (:Person {name: p.name})-[:ENDORSES]->(:Person)-[:ENDORSES]->(:Person)-[:ENDORSES]->(:Person)-[:ENDORSES]->(a:Person)
RETURN DISTINCT a
`

const endorsementDepth5 = `
MATCH (p:Person)
WITH p SKIP %d LIMIT 1
MATCH (:Person {name: p.name})-[:ENDORSES]->(:Person)-[:ENDORSES]->(:Person)-[:ENDORSES]->(:Person)-[:ENDORSES]->(:Person)-[:ENDORSES]->(a:Person)
RETURN DISTINCT a
`

func NewConn() (bolt.Conn, error) {
	driver := bolt.NewDriver()
	return driver.OpenNeo("bolt://neo4j:class@localhost:7687")
}

// func ResetPageRankQuery(conn bolt.Conn) error {
// 	_, err := conn.ExecNeo(setPageRankQuery, nil)
// 	return err
// }

// func RankAlgo(conn bolt.Conn) error {
// 	count := 0
// 	lastRes := 1.0
// 	lastName := ""
// 	i := 0
// 	for ; i < 1; i++ {
// 		_, err := conn.ExecNeo(recursiveRankAlgo, nil)
// 		if err != nil {
// 			return err
// 		}

// 		res, err := TopRanked(conn)
// 		if err != nil {
// 			return err
// 		}

// 		if len(res) > 0 && res[0].name == lastName && res[0].pageRank == lastRes {
// 			count++
// 		} else {
// 			lastName = res[0].name
// 			lastRes = res[0].pageRank
// 		}

// 		if count >= 10 {
// 			break
// 		}
// 	}

// 	log.Println("i count:", i)

// 	return nil
// }

// func TopRanked(conn bolt.Conn) (res []*person, err error) {
// 	rows, err := conn.QueryNeo(topRankedQuery, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for {
// 		data, _, err := rows.NextNeo()
// 		if err != nil {
// 			if err != io.EOF {
// 				return nil, err
// 			}
// 			break
// 		}

// 		p := &person{
// 			id:       data[0].(int64),
// 			name:     data[1].(string),
// 			pageRank: round(data[2].(float64), 0.01),
// 		}

// 		res = append(res, p)
// 	}
// 	log.Println(res)
// 	return res, nil
// }

func RunQuery(query string, conn bolt.Conn) (bolt.Rows, error) {
	return conn.QueryNeo(query, nil)
}

func ExecQuery(query string, conn bolt.Conn) (bolt.Result, error) {
	return conn.ExecNeo(query, nil)
}

type person struct {
	id       int64
	birthday string
	name     string
	pageRank float64
	job      string
}

func round(x, unit float64) float64 {
	return math.Round(x/unit) * unit
}
