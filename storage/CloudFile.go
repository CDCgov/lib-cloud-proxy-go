package storage

type CloudFile struct {
	Bucket   string
	FileName string
	Metadata map[string]string
	Content  string
}
