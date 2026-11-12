// Running the StoreContract suite against PackStore — in the external
// test package since T-228: the suite lives in contenttest (which imports
// content), and the package's internal _test files cannot import it —
// an import cycle in the test. The store factory is the internal
// newPackTestStore factory exported via export_test.go (the stdlib
// pattern).
package content_test

import (
	"testing"

	"crawler/internal/content"
	"crawler/internal/content/contenttest"
)

// TestStoreContractOnPack — the ContentStore contract (T-041) against the
// pack-file implementation: the main check of T-042.
func TestStoreContractOnPack(t *testing.T) {
	contenttest.StoreContract(t, func(t *testing.T) content.ContentStore {
		return content.NewPackStoreForTest(t)
	})
}
