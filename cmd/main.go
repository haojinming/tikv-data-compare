package main

import (
	"fmt"
	"os"
	"tikv-data-compare/checksum"
	"tikv-data-compare/codec"
	"tikv-data-compare/config"
	"tikv-data-compare/diff"
	"tikv-data-compare/scan"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:              "rawkv-data-check",
		Short:            "rawkv-data-check is a data consistency check tool for tikv raw mode.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	config.AddFlags(rootCmd)
	rootCmd.AddCommand(checksum.NewChecksumCommand())
	rootCmd.AddCommand(scan.NewScanCommand())
	rootCmd.AddCommand(diff.NewDiffCommand())
	rootCmd.AddCommand(codec.NewFormatCommand())
	rootCmd.AddCommand(codec.NewDecodeTsCommand())
	rootCmd.AddCommand(codec.NewEncodeKeyCommand())
	// Ouputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("rawkv check failed, %v.\n", err)
		os.Exit(1) // nolint:gocritic
	}
}
