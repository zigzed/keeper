package keeper

import (
	"context"
	"database/sql"
	"fmt"
	"keeper/keeper/rediscacher"
	"testing"
	"time"

	"emperror.dev/errors"
	"github.com/cheekybits/is"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
)

func TestKeeperWithRedis(t *testing.T) {
	is := is.New(t)

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
	})
	is.NotNil(rdb)

	rdb.Del(context.Background(), "test:keeper.1")

	dbx, err := sql.Open("mysql", "btiming:nLQ_3dsa389ADEe903@tcp(192.168.0.114:3306)/btiming?charset=utf8mb4")
	is.NoErr(err)
	is.NotNil(dbx)

	mem := New(rediscacher.New(rdb, nil, nil))
	is.NotNil(mem)

	for i := 0; i < 5; i++ {
		ts := time.Now()

		var results []testDbResult
		ok, err := mem.Get(context.Background(), "test:keeper.1", func(ctx context.Context) (interface{}, time.Duration, error) {
			return slowQuery(ctx, dbx)
		}, &results)
		is.NoErr(err)
		is.True(ok)

		ms := time.Since(ts)
		fmt.Printf("running %d timing: %.6f seconds\n", i, ms.Seconds())
		fmt.Printf("  results: %v\n", results)

		is.Equal(len(results), 10)
	}
}

type testDbResult struct {
	Source   string
	Campaign string
	AppId    string
	Category int
	Revenue  float64
}

func slowQuery(ctx context.Context, dbx *sql.DB) (interface{}, time.Duration, error) {
	query := `
SELECT source, campaign, appId, category, revenue 
FROM zt_offers 
WHERE source = 'polymob'
ORDER BY createdAt
limit 10
	`
	rows, err := dbx.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "query %s failed", query)
	}
	defer rows.Close()

	rs := make([]testDbResult, 0, 20)
	for rows.Next() {
		var r testDbResult
		if err := rows.Scan(&r.Source, &r.Campaign, &r.AppId, &r.Category, &r.Revenue); err != nil {
			return nil, 0, errors.Wrapf(err, "scan %s failed", query)
		}
		rs = append(rs, r)
	}

	return rs, 10 * time.Minute, nil
}
