package dragonfly

import (
	sqt "github.com/iv-menshenin/dragonfly/sql_ast"
)

type (
	depTree struct {
		dependencies sqt.Dependencies
		statements   []sqt.SqlStmt
	}
)

func isSameDependencies(a, b sqt.Dependencies) bool {
	var leftScore, rightScore = 0, 0
	for _, leftDep := range a {
		for _, rightDep := range b {
			if leftDep == rightDep {
				leftScore++
			}
		}
	}
	for _, rightDep := range b {
		for _, leftDep := range a {
			if leftDep == rightDep {
				rightScore++
			}
		}
	}
	return leftScore == len(a) && rightScore == len(b)
}

func addDepTree(t []depTree, d sqt.Dependencies, s sqt.SqlStmt) []depTree {
	for i, dt := range t {
		if isSameDependencies(dt.dependencies, d) {
			t[i].statements = append(t[i].statements, s)
			return t
		}
	}
	return append(t, depTree{
		dependencies: d,
		statements:   []sqt.SqlStmt{s},
	})
}

func popResolvedFrom(t []depTree) ([]depTree, []sqt.SqlStmt) {
	var statements = make([]sqt.SqlStmt, 0, 0)
	var dependencies = make([]depTree, 0, len(t))
	for _, dt := range t {
		if len(dt.dependencies) == 0 {
			statements = append(statements, dt.statements...)
		} else {
			dependencies = append(dependencies, dt)
		}
	}
	return dependencies, statements
}

func resolveInto(t []depTree, resolved sqt.Dependencies) []depTree {
	for i, dt := range t {
		currDep := dt.dependencies
		var newDep sqt.Dependencies
		for _, d := range currDep {
			var isResolved = false
			for _, r := range resolved {
				if r == d {
					isResolved = true
				}
			}
			if !isResolved {
				newDep = append(newDep, d)
			}
		}
		t[i].dependencies = newDep
	}
	return t
}

func fixTheOrderOf(heap []sqt.SqlStmt) []sqt.SqlStmt {
	var (
		dt         []depTree
		statements []sqt.SqlStmt
	)
	for _, statement := range heap {
		dep := sqt.ExploreDependencies(statement)
		dt = addDepTree(dt, dep, statement)
	}
	for {
		var curr []sqt.SqlStmt
		dt, curr = popResolvedFrom(dt)
		if len(curr) == 0 {
			break
		}
		statements = append(statements, curr...)
		for _, r := range curr {
			dt = resolveInto(dt, sqt.ExploreResolved(r))
		}
	}
	for _, deps := range dt {
		statements = append(statements, deps.statements...)
	}
	return statements
}
