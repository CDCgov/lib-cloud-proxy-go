package storage

type CloudFile struct {
	Container string
	FileName  string
	Metadata  map[string]string
	Content   string
}
