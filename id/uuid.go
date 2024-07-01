package id

import (
	"github.com/google/uuid"
)

type UUID struct{}

func (u UUID) New() string {
	return uuid.NewString()
}
