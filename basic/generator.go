package basic

import "github.com/google/uuid"

type Generator interface {
	New() string
}

type UUID struct{}

func (u UUID) New() string {
	return uuid.NewString()
}