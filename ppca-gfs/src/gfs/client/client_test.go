package client

import (
	"fmt"
	"testing"

	"gfs"
)

func updaateSlice(s *[]int) {
	arr := []int{1, 2, 3}
	*s = append(*s, arr...)
}

func TestPassSlice(t *testing.T) {
	s := make([]int, 0)
	updaateSlice(&s)
	fmt.Println(s)
}

func TestClient_getOffsetChunkIndex(t *testing.T) {
	c := &Client{}
	r := c.getOffsetChunkIndex(gfs.Offset(gfs.MaxChunkSize / 2))
	t.Log(r)
}
