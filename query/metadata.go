package query

import "slices"

// Empty reasons for a metadata listing: the dump carries no metadata at all, or none for the metric asked for.
const (
	NoMetadata          = "no-metadata"
	NoMetadataForMetric = "no-metadata-for-metric"
)

// Metadata is a metric's type, unit and help.
type Metadata struct {
	Metric, Type, Unit, Help string
}

// MetadataListing is the metadata for every metric, or for Metric when it isn't empty, and why it's empty when
// it is.
type MetadataListing struct {
	Metric   string
	Metadata []Metadata
	Empty    *Empty
}

// ListMetadata lists all, the dump's metadata, or only metric's when metric isn't empty.
func ListMetadata(all []Metadata, metric string) *MetadataListing {
	l := &MetadataListing{Metric: metric, Metadata: all}
	switch {
	case len(all) == 0:
		l.Empty = &Empty{Reason: NoMetadata, Selectors: []string{}}
	case metric != "":
		l.Metadata = slices.DeleteFunc(slices.Clone(all), func(m Metadata) bool { return m.Metric != metric })
		if len(l.Metadata) == 0 {
			l.Empty = &Empty{Reason: NoMetadataForMetric, Selectors: []string{metric}}
		}
	}
	return l
}
