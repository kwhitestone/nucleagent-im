package model

import "time"

// ExecutorDevice is private to OwnerID. IDs and instance IDs are server-generated.
// Credentials are random scoped values; only their SHA-256 hashes reach the DB.
// PublicKey binds the native installation's Ed25519 proof to the login challenge.
// Short-lived, single-use challenges live in Redis, not in this table.
// Revocation is permanent for a device ID; rebind creates a new ID.
type ExecutorDevice struct {
	ID                string     `json:"id" gorm:"primaryKey;size:64"`
	OwnerID           uint       `json:"-" gorm:"not null;index"`
	InstanceID        string     `json:"-" gorm:"size:64;not null;uniqueIndex"`
	Name              string     `json:"name" gorm:"size:128;not null"`
	OS                string     `json:"os" gorm:"size:16;not null"`
	Arch              string     `json:"arch" gorm:"size:16;not null"`
	PublicKey         string     `json:"-" gorm:"size:64;not null"`
	CredentialHash    string     `json:"-" gorm:"size:64;not null;uniqueIndex"`
	CredentialVersion uint64     `json:"-" gorm:"not null"`
	ExpiresAt         time.Time  `json:"expiresAt" gorm:"not null;index"`
	RevokedAt         *time.Time `json:"revokedAt,omitempty" gorm:"index"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
}
