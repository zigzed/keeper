package keeper

import (
	"context"
	"time"
)

type Cacher interface {
	// Get 根据 key 获取缓存的数据，返回缓存的数据、版本，如果缓存中没有则版本号小于0
	Get(context.Context, string, interface{}) (bool, error)
	// Set 设置 key 对应的值，超时事件，版本号
	Set(context.Context, string, interface{}, time.Duration) error
	// Remove 删除指定 key 的信息
	Remove(context.Context, string) error
}

type SlowQuery func(context.Context) (interface{}, time.Duration, error)

type Keeper struct {
	cacher Cacher
}

func New(cacher Cacher) *Keeper {
	return &Keeper{
		cacher: cacher,
	}
}

func (k *Keeper) Get(ctx context.Context, key string, fn SlowQuery, val interface{}) (bool, error) {
	found, err := k.cacher.Get(ctx, key, val)
	if err != nil {
		return false, err
	}
	if found {
		return true, nil
	}

	res, ttl, err := fn(ctx)
	if err != nil {
		return false, err
	}

	if err := k.cacher.Set(ctx, key, res, ttl); err != nil {
		return false, err
	}

	return k.cacher.Get(ctx, key, val)
}
