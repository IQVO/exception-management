package memory

import (
	"context"
	"sort"

	"github.com/claudioed/exception-management/internal/application/repository"
	"github.com/claudioed/exception-management/internal/domain/exceptioncase"
	"github.com/claudioed/exception-management/internal/domain/exceptionpolicy"
)

// ListPage implements ports.ExceptionCaseRepo: the cases matching the filter,
// NEWEST opened first (then case id descending), strictly after the keyset
// position, at most limit of them — the order and the page the Postgres
// adapter returns too.
func (r *Cases) ListPage(_ context.Context, f repository.CaseFilter, after *repository.CaseAfter, limit int) ([]*exceptioncase.Case, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*exceptioncase.Case
	for _, id := range r.order {
		c, err := r.restore(id)
		if err != nil {
			return nil, err
		}
		if matches(c, f) && isAfter(c, after) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OpenedAt().Equal(out[j].OpenedAt()) {
			return out[i].ID() > out[j].ID()
		}
		return out[i].OpenedAt().After(out[j].OpenedAt())
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// matches reports whether c passes the non-blank filters.
func matches(c *exceptioncase.Case, f repository.CaseFilter) bool {
	k := c.Key()
	return (f.Status == "" || string(c.Status()) == f.Status) &&
		(f.Queue == "" || c.Queue() == f.Queue) &&
		(f.SiteID == "" || k.SiteID == f.SiteID) &&
		(f.SourceType == "" || k.SourceType == f.SourceType)
}

// isAfter reports whether c sorts strictly after the position in the
// (opened-at, id) descending order; a nil position admits every case.
func isAfter(c *exceptioncase.Case, after *repository.CaseAfter) bool {
	if after == nil {
		return true
	}
	if c.OpenedAt().Equal(after.OpenedAt) {
		return c.ID() < after.ID
	}
	return c.OpenedAt().Before(after.OpenedAt)
}

// ListPage implements ports.ExceptionPolicyRepo: the latest version of every
// declared source type, ordered by source type, strictly after the source
// type `after` ("" = from the start), at most limit of them.
func (r *Policies) ListPage(_ context.Context, after string, limit int) ([]*exceptionpolicy.Policy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*exceptionpolicy.Policy, 0, len(r.latest))
	for _, p := range r.latest {
		if after == "" || p.SourceType() > after {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SourceType() < out[j].SourceType()
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
