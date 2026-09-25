package incluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
)

// namespaceFilters publishes complete, immutable snapshots to all resource clients.
// A nil snapshot rejects namespaces until the first valid document is loaded.
type namespaceFilters struct {
	current atomic.Pointer[namespaceFilter]
}
type namespaceFilter struct {
	include, exclude                 []string
	includePatterns, excludePatterns []string
	includeRegex, excludeRegex       []*regexp.Regexp
}

type namespaceList []string

func (l *namespaceList) UnmarshalJSON(data []byte) error {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	switch v := value.(type) {
	case string:
		*l = namespaceList{}
		if v != "" {
			*l = strings.Split(v, ",")
		}
	case []any:
		*l = make(namespaceList, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return fmt.Errorf("namespace list entries must be strings")
			}
			*l = append(*l, s)
		}
	default:
		return fmt.Errorf("namespace list must be a string or an array of strings")
	}
	return nil
}

func (f *namespaceFilters) update(data []byte) (bool, error) {
	var doc struct {
		Include      namespaceList `json:"includeNamespaces"`
		Exclude      namespaceList `json:"excludeNamespaces"`
		IncludeRegex namespaceList `json:"includeNamespacesRegex"`
		ExcludeRegex namespaceList `json:"excludeNamespacesRegex"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return false, fmt.Errorf("invalid namespace filters: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return false, fmt.Errorf("namespace filters must contain exactly one JSON document")
	}
	if doc.Include == nil || doc.Exclude == nil {
		return false, fmt.Errorf("includeNamespaces and excludeNamespaces are required")
	}
	next := &namespaceFilter{include: doc.Include, exclude: doc.Exclude, includePatterns: doc.IncludeRegex, excludePatterns: doc.ExcludeRegex}
	for _, entry := range []struct {
		patterns []string
		compiled *[]*regexp.Regexp
	}{
		{doc.IncludeRegex, &next.includeRegex}, {doc.ExcludeRegex, &next.excludeRegex},
	} {
		for _, pattern := range entry.patterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			compiled, err := regexp.Compile(pattern)
			if err != nil {
				return false, fmt.Errorf("invalid namespace regex: %w", err)
			}
			*entry.compiled = append(*entry.compiled, compiled)
		}
	}
	previous := f.current.Load()
	if previous != nil && slices.Equal(previous.include, next.include) && slices.Equal(previous.exclude, next.exclude) && slices.Equal(previous.includePatterns, next.includePatterns) && slices.Equal(previous.excludePatterns, next.excludePatterns) {
		return false, nil
	}
	f.current.Store(next)
	return true, nil
}

func (f *namespaceFilters) skip(ns string) bool {
	current := f.current.Load()
	if current == nil {
		return true
	}
	matches := func(names []string, patterns []*regexp.Regexp) bool {
		if slices.Contains(names, ns) {
			return true
		}
		for _, pattern := range patterns {
			if pattern.MatchString(ns) {
				return true
			}
		}
		return false
	}
	if len(current.include) > 0 || len(current.includeRegex) > 0 {
		return !matches(current.include, current.includeRegex)
	}
	return matches(current.exclude, current.excludeRegex)
}
