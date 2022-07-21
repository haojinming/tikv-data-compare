package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/migration/br/pkg/conn"
	"github.com/tikv/migration/br/pkg/pdutil"
	"github.com/tikv/migration/br/pkg/utils"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/errgroup"
)

const (
	flagSrcPD    = "src-pd"
	flagDstPD    = "dst-pd"
	flagStartKey = "start-key"
	flagEndKey   = "end-key"
	flagFormat   = "format"

	CHECKSUM_CONCURRENCY uint = 1
)

type Config struct {
	SrcPD    string `json:"src-pd"`
	DstPD    string `json:"dst-pd"`
	StartKey []byte `json:"start-key"`
	EndKey   []byte `json:"end-key"`
}

func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String(flagSrcPD, "", "Specify the upstream pd address")
	cmd.PersistentFlags().String(flagDstPD, "", "Specify the downstream pd address")
	cmd.PersistentFlags().String(flagStartKey, "", "Specify the range start")
	cmd.PersistentFlags().String(flagEndKey, "", "Specify the range end")
	cmd.PersistentFlags().String(flagFormat, "raw", "Specify the start/end key format, suppot 'hex/raw/escaped'")
}

func (cfg *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	if cfg.SrcPD, err = flags.GetString(flagSrcPD); err != nil {
		return errors.Trace(err)
	}
	if cfg.DstPD, err = flags.GetString(flagDstPD); err != nil {
		return errors.Trace(err)
	}
	format, err := flags.GetString(flagFormat)
	if err != nil {
		return errors.Trace(err)
	}
	startKey, err := flags.GetString(flagStartKey)
	if err != nil {
		return errors.Trace(err)
	}
	endKey, err := flags.GetString(flagEndKey)
	if err != nil {
		return errors.Trace(err)
	}
	if cfg.StartKey, err = utils.ParseKey(format, startKey); err != nil {
		return errors.Trace(err)
	}
	if cfg.EndKey, err = utils.ParseKey(format, endKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func GetClusterAPIVersion(ctx context.Context, pdAddr string) (kvrpcpb.APIVersion, error) {
	pdCtrl, err := pdutil.NewPdController(ctx, pdAddr, nil, pd.SecurityOption{})
	if err != nil {
		fmt.Printf("new pd client fail, %v.\n", err)
		return kvrpcpb.APIVersion_V1, err
	}
	apiVersion, err := conn.GetTiKVApiVersion(ctx, pdCtrl.GetPDClient(), nil)
	if err != nil {
		fmt.Printf("get api version fail, %v.\n", err)
		return kvrpcpb.APIVersion_V1, err
	}
	return apiVersion, nil
}

func GetChecksum(ctx context.Context, pdAddr string, apiVersion kvrpcpb.APIVersion, startKey, endKey []byte) (rawkv.RawChecksum, error) {
	rawkvClient, err := rawkv.NewClientWithOpts(ctx, strings.Split(pdAddr, ","), rawkv.WithAPIVersion(apiVersion))
	if err != nil {
		fmt.Printf("new rawkv client fails, %v.\n", err)
		return rawkv.RawChecksum{}, err
	}
	storageChecksum, err := rawkvClient.Checksum(ctx, startKey, endKey)
	if err != nil {
		return rawkv.RawChecksum{}, err
	}
	return storageChecksum, nil
}

func runRawKvChecksum(cmd *cobra.Command, name string) error {
	cfg := Config{}
	err := cfg.ParseFromFlags(cmd.Flags())
	if err != nil {
		fmt.Printf("parse cmd flags error, %v.\n", err)
		return err
	}
	ctx := context.Background()
	srcAPIVersion, err := GetClusterAPIVersion(ctx, cfg.SrcPD)
	if err != nil {
		return err
	}
	dstAPIVersion, err := GetClusterAPIVersion(ctx, cfg.DstPD)
	if err != nil {
		return err
	}
	if srcAPIVersion != dstAPIVersion {
		fmt.Printf("Different api version between src:%v and dst:%v.\n", srcAPIVersion, dstAPIVersion)
		return errors.Errorf("Different api version.")
	}
	srcChecksum := rawkv.RawChecksum{}
	dstChecksum := rawkv.RawChecksum{}
	errGroup := new(errgroup.Group)
	errGroup.Go(func() error {
		srcChecksum, err = GetChecksum(ctx, cfg.SrcPD, srcAPIVersion, cfg.StartKey, cfg.EndKey)
		if err != nil {
			fmt.Printf("calc src cluster checksum fails, %v.\n", err)
			return err
		}
		fmt.Printf("src cluster checksum, %v.\n", srcChecksum)
		return nil
	})
	errGroup.Go(func() error {
		dstChecksum, err = GetChecksum(ctx, cfg.DstPD, dstAPIVersion, cfg.StartKey, cfg.EndKey)
		if err != nil {
			fmt.Printf("calc dst cluster checksum fails, %v.\n", err)
			return err
		}
		fmt.Printf("dst cluster checksum, %v.\n", dstChecksum)
		return nil
	})
	err = errGroup.Wait()
	if err != nil {
		return err
	}
	if srcChecksum != dstChecksum {
		fmt.Printf("src and dst checksum are not the same, src:%v, dst:%v.\n", srcChecksum, dstChecksum)
		return errors.Errorf("checksum mismatch.\n")
	}
	fmt.Printf("src and dst checksum are the same, %v.\n", srcChecksum)
	return nil
}

func main() {
	rootCmd := &cobra.Command{
		Use:              "tikv-data-compare",
		Short:            "tikv-data-compare is a data consistency check tool for tikv raw mode.",
		TraverseChildren: true,
		SilenceUsage:     true,
		Args:             cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvChecksum(cmd, "RawCheck")
		},
	}
	AddFlags(rootCmd)
	// Ouputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("rawkv check failed, %v.\n", err)
		os.Exit(1) // nolint:gocritic
	}
}
