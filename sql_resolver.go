package dragonfly

import (
	sqt "github.com/iv-menshenin/dragonfly/sql_ast"
	"sort"
)

type sqlDepended []sqt.SqlStmt

func (p sqlDepended) Len() int {
	return len(p)
}

// TODO
func (p sqlDepended) Less(i, j int) bool {
	depI := sqt.ExploreDependencies(p[i])
	depJ := sqt.ExploreDependencies(p[j])
	resI := sqt.ExploreResolved(p[i])
	resJ := sqt.ExploreResolved(p[j])
	scoreI, scoreJ := 0, 0
	for _, dep := range depI {
		for _, res := range resJ {
			if dep == res {
				scoreJ++
			}
		}
	}
	for _, dep := range depJ {
		for _, res := range resI {
			if dep == res {
				scoreI++
			}
		}
	}
	if scoreI == scoreJ {
		return p[i].String() < p[j].String()
	} else {
		return scoreI > scoreJ
	}
}

func (p sqlDepended) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func fixTheOrderOf(heap []sqt.SqlStmt) {
	sort.Sort(sqlDepended(heap))
}
