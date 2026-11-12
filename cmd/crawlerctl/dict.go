// ---------------------------------------------------------------- zstd dictionary
//
// Command for managing the content compression dictionary (spec 5.7,
// spec 13.2: compression parameters are at the developers' discretion).
// Training and retraining are the same action (train): the dictionary
// file is overwritten atomically and the services pick it up on the next
// start via the content.zstd_dictionary path. On retraining the previous
// dictionary is archived into the retired-dictionary registry
// (dict-<id>.zstd next to the new file, T-103) — the already written
// content stays readable. Fully implemented.

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"

	"crawler/internal/content"
)

// dictHelp — help text for crawlerctl dict --help.
const dictHelp = `crawlerctl dict — managing the zstd compression dictionary (spec 5.7)

Usage:
  crawlerctl dict train <output-file> [--samples PATH]…

Trains a zstd dictionary over HTML samples (the repeated patterns of
parkings and stubs go into the dictionary and are not encoded in every
page) and writes it to the output file atomically. The --samples path is
a *.html/*.htm file or a directory (a recursive walk); the flag can be
repeated. The default samples are testdata/laravel-samples. Retraining —
running the command again.

The dictionary is wired in by the content.zstd_dictionary configuration.
On retraining the previous dictionary is automatically archived into the
previous-dictionaries registry (a dict-<id>.zstd file next to the new one):
the packs written with it stay readable.
`

// dictTrainSampleDefault — default sample directory (same as test-rules):
// in the tools dev container the repository is mounted at /src.
const dictTrainSampleDefault = "testdata/laravel-samples"

func cmdDict(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("specify an action: crawlerctl dict train")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "train":
		return dictTrain(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", dictHelp)
		return flag.ErrHelp
	default:
		return usagef("unknown action %q (valid: train)", action)
	}
}

func dictTrain(args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "help" {
		out(stdout, "%s", dictHelp)
		return flag.ErrHelp
	}
	fs := newFlagSet("dict train")
	var samplePaths []string
	fs.Func("samples", "a file or directory with HTML samples (repeatable)",
		func(v string) error {
			samplePaths = append(samplePaths, v)
			return nil
		})
	// The standard flag package stops parsing at the first positional
	// argument, while the help puts the output file before --samples:
	// flags (and their values) are moved to the front, positionals to
	// the end.
	var pass, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			// Flag terminator: everything after it is a positional argument.
			pos = append(pos, args[i+1:]...)
			i = len(args)
		case a == "-h" || a == "--help":
			pass = append(pass, a)
		case a == "-samples" || a == "--samples":
			if i+1 >= len(args) {
				return usagef("dict train: the --samples flag requires a value")
			}
			i++
			pass = append(pass, "--samples", args[i])
		case strings.HasPrefix(a, "-samples=") || strings.HasPrefix(a, "--samples="):
			pass = append(pass, a)
		case strings.HasPrefix(a, "-"):
			pass = append(pass, a) // unknown flag — a parse error carrying the name
		default:
			pos = append(pos, a)
		}
	}
	if handled, err := parseFlags(fs, dictHelp, pass, stdout); handled {
		return err
	}
	if len(pos) != 1 {
		return usagef("specify the output file: crawlerctl dict train <file> [--samples PATH]…")
	}
	outPath := pos[0]
	if len(samplePaths) == 0 {
		samplePaths = []string{dictTrainSampleDefault}
	}

	files, err := collectSampleFiles(samplePaths)
	if err != nil {
		return fmt.Errorf("dict train: %w", err)
	}
	if len(files) == 0 {
		return fmt.Errorf("dict train: no HTML samples in %s (*.html, *.htm)",
			strings.Join(samplePaths, ", "))
	}
	samples, read := readSamples(files, content.DefaultTrainCorpusLimit)
	if len(samples) == 0 {
		return fmt.Errorf("dict train: the samples are empty")
	}
	out(stdout, "dict train: sample files %d of %d found, corpus %d bytes (the training limit %d bytes)\n",
		len(samples), len(files), read, content.DefaultTrainCorpusLimit)

	d, err := content.TrainDict(samples, content.TrainOptions{})
	if err != nil {
		return fmt.Errorf("dict train: %w", err)
	}
	out(stdout, "dict train: the dictionary trained: %d bytes, ID %08x\n", len(d.Raw()), d.ID())

	// Measurement on the training corpus — a reference point for the
	// operator (the real effect is measured on fresh pages of the same
	// template).
	var plainSum, dictSum, evalBytes int
	for _, s := range samples {
		plainSum += len(content.Compress(s))
		dictSum += len(d.Compress(s))
		evalBytes += len(s)
	}
	out(stdout, "dict train: the corpus compression estimate over %d bytes: without the dictionary %d, with it %d\n",
		evalBytes, plainSum, dictSum)

	// The previous dictionary goes to the retired registry (T-103) before
	// the new file is written: an archiving failure leaves the previous
	// one active. Retraining does not make written content unreadable —
	// the previous dictionary is read back via lazy re-registration.
	// Same ID (same corpus) — nothing to archive.
	if prev, err := content.LoadDict(outPath); err == nil {
		if prev.ID() != d.ID() {
			regPath := content.DictRegistryPath(filepath.Dir(outPath), prev.ID())
			if err := content.RetireDict(prev, filepath.Dir(outPath)); err != nil {
				return fmt.Errorf("dict train: archiving the previous dictionary %08x into %s: %w",
					prev.ID(), regPath, err)
			}
			out(stdout, "dict train: the previous dictionary ID %08x — in the registry: %s\n", prev.ID(), regPath)
		}
	} else if !errors.Is(err, iofs.ErrNotExist) {
		// The previous file is not a dictionary: it cannot make old packs
		// readable, and the new one will be written over it — the
		// operator should know.
		out(stderr, "dict train: the previous file %s is not a zstd dictionary (%v), archiving skipped\n",
			outPath, err)
	}

	if err := d.Save(outPath); err != nil {
		return fmt.Errorf("dict train: %w", err)
	}
	out(stdout, "dict train: the dictionary written: %s\n", outPath)
	return nil
}

// collectSampleFiles collects HTML file paths: each path is a file or a
// directory (recursive walk, only *.html/*.htm). The order is lexical
// (WalkDir) — training is deterministic.
func collectSampleFiles(paths []string) ([]string, error) {
	var files []string
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			// The samples path is named in the error, not the raw os error:
			// "stat …: no such file" without context does not say it is
			// --samples.
			return nil, fmt.Errorf("samples %s: %w", p, err)
		}
		if !st.IsDir() {
			if isHTMLFile(p) {
				files = append(files, p)
			}
			continue
		}
		err = filepath.WalkDir(p, func(path string, d iofs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.Type().IsRegular() && isHTMLFile(path) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return files, nil
}

// isHTMLFile — the file extension is .html/.htm.
func isHTMLFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".html", ".htm":
		return true
	}
	return false
}

// readSamples reads files in order until the total size exceeds limit:
// retraining memory is capped even on a huge sample directory.
func readSamples(files []string, limit int) ([][]byte, int) {
	samples := make([][]byte, 0, len(files))
	var total int
	for _, f := range files {
		if total >= limit {
			break
		}
		b, err := os.ReadFile(f)
		if err != nil {
			continue // skip an unreadable sample: training must not fail because of one file
		}
		samples = append(samples, b)
		total += len(b)
	}
	return samples, total
}
