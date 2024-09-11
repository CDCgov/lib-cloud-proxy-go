package storage

type CloudFile struct {
	bucket   string
	fileName string
	metadata map[string]string
	content  string
}
