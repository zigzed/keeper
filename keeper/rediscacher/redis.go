package rediscacher

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"time"

	"emperror.dev/errors"
	"github.com/go-redis/redis/v8"
	"github.com/klauspost/compress/zstd"
)

type Encoder func(interface{}) ([]byte, error)
type Decoder func([]byte, interface{}) error

type cacher struct {
	conn    redis.UniversalClient
	encoder Encoder
	decoder Decoder
}

func New(conn redis.UniversalClient, encoder Encoder, decoder Decoder) *cacher {
	if encoder == nil && decoder == nil {
		// encoder = json.Marshal
		// decoder = json.Unmarshal
		encoder = func(v interface{}) ([]byte, error) {
			b := new(bytes.Buffer)
			e := gob.NewEncoder(b)
			if err := e.Encode(v); err != nil {
				return nil, err
			}
			return b.Bytes(), nil
		}
		decoder = func(b []byte, v interface{}) error {
			d := gob.NewDecoder(bytes.NewReader(b))
			if err := d.Decode(v); err != nil {
				return err
			}
			return nil
		}
	}
	return &cacher{
		conn:    conn,
		encoder: encoder,
		decoder: decoder,
	}
}

func (c *cacher) Get(ctx context.Context, key string, val interface{}) (bool, error) {
	buf, err := c.conn.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrapf(err, "get key %s from redis failed", key)
	}

	pbuf, err := uncompress(buf)
	if err != nil {
		return false, errors.Wrapf(err, "get key %s from redis uncompress failed", key)
	}

	if err := c.decoder(pbuf, val); err != nil {
		return false, errors.Wrapf(err, "get key %s from redis decode failed", key)
	}

	return true, nil
}

func (c *cacher) Set(ctx context.Context, key string, val interface{}, ttl time.Duration) error {
	buf, err := c.encoder(val)
	if err != nil {
		return errors.Wrapf(err, "sett key %s to redis encode failed", key)
	}

	cbuf, err := compress(buf)
	if err != nil {
		return errors.Wrapf(err, "set key %s to redis compress failed", key)
	}

	if _, err := c.conn.Set(ctx, key, cbuf, ttl).Result(); err != nil {
		return errors.Wrapf(err, "set key %s to redis failed", key)
	}

	return nil
}

func (c *cacher) Remove(ctx context.Context, key string) error {
	if _, err := c.conn.Del(ctx, key).Result(); err != nil {
		return errors.Wrapf(err, "del key %s to redis failed", key)
	}

	return nil
}

func compress(input []byte) ([]byte, error) {
	var (
		zout  = new(bytes.Buffer)
		zw, _ = zstd.NewWriter(zout)
	)
	if _, err := io.Copy(zw, bytes.NewReader(input)); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}

	return zout.Bytes(), nil

	// return input, nil
}

func uncompress(input []byte) ([]byte, error) {
	var (
		pout  = new(bytes.Buffer)
		zr, _ = zstd.NewReader(bytes.NewReader(input))
	)

	if _, err := io.Copy(pout, zr); err != nil {
		return nil, err
	}

	return pout.Bytes(), nil
	// return input, nil
}
