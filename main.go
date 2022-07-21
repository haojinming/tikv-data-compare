package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/util/codec"
	"github.com/tikv/migration/br/pkg/conn"
	"github.com/tikv/migration/br/pkg/gluetikv"
	"github.com/tikv/migration/br/pkg/pdutil"
	"github.com/tikv/migration/br/pkg/restore"
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

func UpdateChecksum(checksum *rawkv.RawChecksum, another *rawkv.RawChecksum) {
	checksum.Crc64Xor ^= another.Crc64Xor
	checksum.TotalKvs += another.TotalKvs
	checksum.TotalBytes += another.TotalBytes
}

func ConvertToRawKey(key []byte, apiVersion kvrpcpb.APIVersion) []byte {
	if apiVersion != kvrpcpb.APIVersion_V2 {
		return key
	}
	rawkey, _, _ := codec.DecodeBytes(key, nil)
	return rawkey
}

func GetChecksum(ctx context.Context, pdAddr string, startKey, endKey []byte) (rawkv.RawChecksum, error) {
	pdCtrl, err := pdutil.NewPdController(ctx, pdAddr, nil, pd.SecurityOption{})
	if err != nil {
		fmt.Printf("new pd client fail, %v.\n", err)
		return rawkv.RawChecksum{}, err
	}
	pdClient := pdCtrl.GetPDClient()
	apiVersion, err := conn.GetTiKVApiVersion(ctx, pdClient, nil)
	if err != nil {
		fmt.Printf("get api version, %v.\n", err)
		return rawkv.RawChecksum{}, err
	}

	splitClient := restore.NewSplitClient(pdClient, nil, true)
	regionInfo, err := restore.PaginateScanRegion(ctx, splitClient, startKey, endKey, 1024)
	if err != nil {
		fmt.Printf("get regions fail, %v.\n", err)
		return rawkv.RawChecksum{}, err
	}
	keyRanges := []*utils.KeyRange{}
	for _, region := range regionInfo {
		keyRange := utils.KeyRange{
			Start: ConvertToRawKey(region.Region.StartKey, apiVersion),
			End:   ConvertToRawKey(region.Region.EndKey, apiVersion),
		}
		keyRanges = append(keyRanges, &keyRange)
	}
	rawkvClient, err := rawkv.NewClientWithOpts(ctx, strings.Split(pdAddr, ","), rawkv.WithAPIVersion(apiVersion))
	if err != nil {
		fmt.Printf("new rawkv client fails, %v.\n", err)
		return rawkv.RawChecksum{}, err
	}
	glue := new(gluetikv.Glue)
	updateCh := glue.StartProgress(ctx, "Raw Checksum", int64(len(keyRanges)), false)
	fmt.Println("total ranges", len(keyRanges))
	progressCallBack := func() {
		// fmt.Println("checksum finish one")
		updateCh.Inc()
	}
	storageChecksum := rawkv.RawChecksum{}
	lock := sync.Mutex{}
	workerPool := utils.NewWorkerPool(CHECKSUM_CONCURRENCY, "Ranges")
	eg, _ := errgroup.WithContext(ctx)
	for _, r := range keyRanges {
		keyRange := r // copy to another variable in case it's overwritten
		workerPool.ApplyOnErrorGroup(eg, func() error {
			ret, err := rawkvClient.Checksum(ctx, keyRange.Start, keyRange.End)
			if err != nil {
				return err
			}
			lock.Lock()
			UpdateChecksum(&storageChecksum, &ret)
			// fmt.Println("checksum finish one here")
			progressCallBack()
			lock.Unlock()
			return nil
		})
	}
	err = eg.Wait()
	if err != nil {
		return rawkv.RawChecksum{}, err
	}
	updateCh.Close()
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
	srcChecksum, err := GetChecksum(ctx, cfg.SrcPD, cfg.StartKey, cfg.EndKey)
	if err != nil {
		fmt.Printf("calc src cluster checksum fails, %v.\n", err)
		return err
	}
	dstChecksum, err := GetChecksum(ctx, cfg.DstPD, cfg.StartKey, cfg.EndKey)
	if err != nil {
		fmt.Printf("calc dst cluster checksum fails, %v.\n", err)
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
		Use:              "rawkv-data-check",
		Short:            "rawkv-data-check is a data consistency check tool for tikv raw mode.",
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
