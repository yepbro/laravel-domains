// Export of test factories to the external test package content_test —
// the stdlib export_test.go pattern. Needed since T-228: the contract
// suite StoreContract moved to the contenttest package, which imports
// content, so the package's internal _test files cannot import contenttest
// (an import cycle in the test: content+tests → contenttest → content),
// while the external content_test can (go rebinds its dependency on
// content to the test variant of the package). The file exists only in
// the test build (the _test.go suffix) and never reaches the production
// binary.
package content

import "testing"

// NewPackStoreForTest — a PackStore on a fake pgx pool with real pack
// files in a temporary directory (newPackTestStore, pack_test.go):
// for contenttest.StoreContract in the external test package
// (pack_contract_test.go).
func NewPackStoreForTest(t *testing.T) *PackStore {
	st, _ := newPackTestStore(t, nil)
	return st
}
