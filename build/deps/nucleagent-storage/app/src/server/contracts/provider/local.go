package provider

const LocalFilePrefix = "file://"
const BlobPath = "/blob"

func MakeLocalURL(full string) string { return LocalFilePrefix + full }

// LocalBlobProvider is the narrow signed-byte capability; no concrete disk implementation leaks into blob.
type LocalBlobProvider interface {
	Provider
	Verify(method, full string, expires, max int64, signature string) error
	ResolvePath(full string) (string, error)
}
