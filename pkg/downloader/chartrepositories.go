package downloader

import (
	"maps"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"helm.sh/helm/v3/internal/urlutil"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/helmpath"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/repo"
)

type ChartRepositories struct {
	indexPath string
	indices   map[string]*repo.IndexFile
	indicesM  sync.Mutex
	repos     map[string]*repo.Entry
	reposM    sync.RWMutex
}

func (c *ChartRepositories) GetIndex(name string) (*repo.IndexFile, error) {
	if name == "" {
		return nil, nil
	}
	c.indicesM.Lock()
	defer c.indicesM.Unlock()
	if index, ok := c.indices[name]; ok {
		return index, nil
	}
	repoEntry := c.getInfo(name)
	if repoEntry == nil {
		return nil, nil
	}
	index, err := repo.LoadIndexFile(filepath.Join(c.indexPath, helmpath.CacheIndexFile(repoEntry.Name)))
	if err != nil {
		return nil, err
	}
	c.indices[name] = index
	return index, nil
}

func (c *ChartRepositories) GetInfo(name string) *repo.Entry {
	c.reposM.RLock()
	defer c.reposM.RUnlock()
	return c.getInfo(name)
}

func (c *ChartRepositories) getInfo(name string) *repo.Entry {
	if name == "" {
		return nil
	}
	result, _ := c.repos[name]
	return result
}

func (c *ChartRepositories) Keys() []string {
	c.reposM.RLock()
	defer c.reposM.RUnlock()
	return slices.Collect(maps.Keys(c.repos))
}

func (c *ChartRepositories) getInfoByF(f func(*repo.Entry) bool) *repo.Entry {
	for _, entry := range c.repos {
		if f(entry) {
			return entry
		}
	}
	return nil
}

func (c *ChartRepositories) GetKeysByF(f func(*repo.Entry) bool) []string {
	c.reposM.RLock()
	defer c.reposM.RUnlock()
	result := []string{}
	for _, entry := range c.repos {
		if f(entry) {
			result = append(result, entry.Name)
		}
	}
	return result
}

func (c *ChartRepositories) GetInfoByURL(url string) *repo.Entry {
	c.reposM.Lock()
	defer c.reposM.Unlock()
	return c.getInfoByURL(url)
}
func (c *ChartRepositories) getInfoByURL(url string) *repo.Entry {
	if entry := c.getInfoByF(func(entry *repo.Entry) bool {
		return urlutil.Equal(entry.URL, url)
	}); entry != nil {
		return entry
	}
	// If we don't have a named repo, assume that the repository is a URL to a public chart repository.
	generatedName, err := key(url)
	if err != nil {
		return nil
	}
	generatedName = managerKeyPrefix + generatedName
	generatedRepo := &repo.Entry{
		Name: generatedName,
		URL:  url,
	}
	c.repos[generatedName] = generatedRepo
	return generatedRepo
}

func NewChartRepositories(repoConfigPath, repoCachePath string) (*ChartRepositories, error) {
	repoFile, err := loadRepoConfig(repoConfigPath)
	if err != nil {
		return nil, err
	}

	repos := make(map[string]*repo.Entry, len(repoFile.Repositories))
	for _, entry := range repoFile.Repositories {
		repos[entry.Name] = entry
	}
	return &ChartRepositories{
		repos:     repos,
		indexPath: repoCachePath,
		indices:   map[string]*repo.IndexFile{},
	}, nil
}

// GetForDep returns a Key corresponding to the Repository config for a given
// chart.Dependency.
//
// This key can be used in *ChartRepositories.GetInfo and
// *ChartRepositories.GetIndex(). GetForDep return an empty string if the
// dependency does not refer to a Repository config.
func (c *ChartRepositories) CanonicalizeRepoName(name string) string {
	// If the repo is blank, is a local file, or OCI, we don't have a config for it.
	if name == "" || strings.HasPrefix(name, "file://") || registry.IsOCI(name) {
		return ""
	}
	c.reposM.Lock()
	defer c.reposM.Unlock()
	// If we have a named repo, return that
	if trimmed := strings.TrimPrefix(name, "@"); c.getInfo(trimmed) != nil {
		return trimmed
	}
	if trimmed := strings.TrimPrefix(name, "alias:"); c.getInfo(trimmed) != nil {
		return trimmed
	}
	if u, err := url.Parse(name); err == nil && u.IsAbs() {
		if entry := c.getInfoByURL(name); entry != nil {
			return entry.Name
		}
	}
	return ""
}

func (c *ChartRepositories) GetForDeps(deps []*chart.Dependency) map[string]string {
	out := map[string]string{}
	for _, dep := range deps {
		if name := c.CanonicalizeRepoName(dep.Repository); name != "" {
			out[dep.Name] = name
		}
	}
	return out
}

func (c *ChartRepositories) GetForRef(ref string) string {
	i := strings.LastIndex(ref, "/")
	if i > 0 {
		return c.CanonicalizeRepoName(ref[:i])
	}
	return ""
}
