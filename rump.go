package main

import (
	"os"
	"fmt"
	"flag"
	"github.com/gomodule/redigo/redis"
)

// Report all errors to stdout.
func handle(err error) {
	if err != nil && err != redis.ErrNil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func connection(url string, db string, password string) (redis.Conn, error) {
	c, err := redis.Dial("tcp", url)
    if err != nil {
		return nil, err
	}
	if password != "" {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return nil, err
		}
	}
	if _, err := c.Do("SELECT", db); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Scan and queue source keys.
func get(conn redis.Conn, queue chan<- map[string]string) {
	var (
		cursor int64
		keys []string
	)

	for {
		// Scan a batch of keys.
		values, err := redis.Values(conn.Do("SCAN", cursor))
		handle(err)
		values, err = redis.Scan(values, &cursor, &keys)
		handle(err)

		// Get pipelined dumps.
		for _, key := range keys {
			conn.Send("DUMP", key)
		}
		dumps, err := redis.Strings(conn.Do(""))
		handle(err)

		// Build batch map.
		batch := make(map[string]string)
		for i, _ := range keys {
			batch[keys[i]] = dumps[i]
		}

		// Last iteration of scan.
		if cursor == 0 {
			// queue last batch.
			select {
			case queue <- batch:
			}
			close(queue)
			break
		}

		fmt.Printf(">")
		// queue current batch.
		queue <- batch
	}
}

// Restore a batch of keys on destination.
func put(conn redis.Conn, queue <-chan map[string]string) {
	for batch := range queue {
		for key, value := range batch {
			conn.Send("RESTORE", key, "0", value)
		}
		_, err := conn.Do("")
		handle(err)

		fmt.Printf(".")
	}
}

func main() {
	from := flag.String("from", "", "example: 127.0.0.1:6379")
	to := flag.String("to", "", "example: 127.0.0.1:6379")
	from_pwd := flag.String("from_pwd", "", "from redis password")
	to_pwd := flag.String("to_pwd", "", "to redis password")
	from_db := flag.String("from_db", "", "from db")
	to_db := flag.String("to_db", "", "to db")
	flag.Parse()

	source, err := connection(*from, *from_db, *from_pwd)
	handle(err)
	destination, err := connection(*to, *to_db, *to_pwd)
	handle(err)
	defer source.Close()
	defer destination.Close()

	// Channel where batches of keys will pass.
	queue := make(chan map[string]string, 100)

	// Scan and send to queue.
	go get(source, queue)

	// Restore keys as they come into queue.
	put(destination, queue)

	fmt.Println("Sync done.")
}
