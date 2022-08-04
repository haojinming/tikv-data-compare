package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

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
	flagSrcPD       = "src-pd"
	flagDstPD       = "dst-pd"
	flagStartKey    = "start-key"
	flagEndKey      = "end-key"
	flagFormat      = "format"
	flagParallel    = "use-parallel"
	flagConcurrency = "concurrency"
)

type Config struct {
	SrcPD       string `json:"src-pd"`
	DstPD       string `json:"dst-pd"`
	StartKey    []byte `json:"start-key"`
	EndKey      []byte `json:"end-key"`
	UseParallel bool   `json:"use-parallel"`
	Concurrency uint   `json:"concurrency"`
}

func AddFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String(flagSrcPD, "", "Specify the upstream pd address")
	cmd.PersistentFlags().String(flagDstPD, "", "Specify the downstream pd address")
	cmd.PersistentFlags().String(flagStartKey, "", "Specify the range start")
	cmd.PersistentFlags().String(flagEndKey, "", "Specify the range end")
	cmd.PersistentFlags().String(flagFormat, "raw", "Specify the start/end key format, suppot 'hex/raw/escaped'")
	cmd.PersistentFlags().Bool(flagParallel, true, "Use parallel to do checksum.")
	cmd.PersistentFlags().Uint(flagConcurrency, 10, "Concurrency of checksum.")
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
	if cfg.UseParallel, err = flags.GetBool(flagParallel); err != nil {
		return errors.Trace(err)
	}
	if cfg.Concurrency, err = flags.GetUint(flagConcurrency); err != nil {
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
	_, rawkey, _ := codec.DecodeBytes(key, nil)
	if len(rawkey) > 0 {
		if len(rawkey) < 4 {
			panic("wrong key format")
		}
		rawkey = rawkey[4:]
	}
	return rawkey
}

func GetChecksumParallel(ctx context.Context, cmd, pdAddr string, apiVersion kvrpcpb.APIVersion, startKey, endKey []byte, concurrency uint) (rawkv.RawChecksum, error) {
	pdCtrl, err := pdutil.NewPdController(ctx, pdAddr, nil, pd.SecurityOption{})
	if err != nil {
		fmt.Printf("new pd client fail, %v.\n", err)
		return rawkv.RawChecksum{}, err
	}
	defer pdCtrl.Close()
	splitClient := restore.NewSplitClient(pdCtrl.GetPDClient(), nil, true)
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
	updateCh := glue.StartProgress(ctx, "Raw Checksum "+cmd, int64(len(keyRanges)), false)
	progressCallBack := func() {
		updateCh.Inc()
	}
	storageChecksum := rawkv.RawChecksum{}
	lock := sync.Mutex{}
	workerPool := utils.NewWorkerPool(concurrency, "Ranges")
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
			fmt.Printf("checksum finish on range:%s-%s, checksum:%v.",
				hex.EncodeToString(keyRange.Start), hex.EncodeToString(keyRange.End), ret)
			lock.Unlock()
			progressCallBack()
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

func GetClusterAPIVersion(ctx context.Context, pdAddr string) (kvrpcpb.APIVersion, error) {
	pdCtrl, err := pdutil.NewPdController(ctx, pdAddr, nil, pd.SecurityOption{})
	if err != nil {
		fmt.Printf("new pd client fail, %v.\n", err)
		return kvrpcpb.APIVersion_V1, err
	}
	defer pdCtrl.Close()
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
	defer rawkvClient.Close()
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
		if cfg.UseParallel {
			srcChecksum, err = GetChecksumParallel(ctx, "Src", cfg.SrcPD, srcAPIVersion, cfg.StartKey, cfg.EndKey, cfg.Concurrency)
		} else {
			srcChecksum, err = GetChecksum(ctx, cfg.SrcPD, srcAPIVersion, cfg.StartKey, cfg.EndKey)
		}

		if err != nil {
			fmt.Printf("calc src cluster checksum fails, %v.\n", err)
			return err
		}
		fmt.Printf("src cluster checksum, %v.\n", srcChecksum)
		return nil
	})
	errGroup.Go(func() error {
		if cfg.UseParallel {
			dstChecksum, err = GetChecksumParallel(ctx, "Dst", cfg.DstPD, dstAPIVersion, cfg.StartKey, cfg.EndKey, cfg.Concurrency)
		} else {
			dstChecksum, err = GetChecksum(ctx, cfg.DstPD, dstAPIVersion, cfg.StartKey, cfg.EndKey)
		}

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

func RawKVScan(ctx context.Context, pdAddr, name string, apiVersion kvrpcpb.APIVersion, startKey, endKey []byte) error {
	rawkvClient, err := rawkv.NewClientWithOpts(ctx, strings.Split(pdAddr, ","), rawkv.WithAPIVersion(apiVersion))
	if err != nil {
		fmt.Printf("new rawkv client fails, %v.\n", err)
		return err
	}
	defer rawkvClient.Close()

	pwd, err := os.Getwd()
	if err != nil {
		return err
	}

	filePath := filepath.Join(pwd, time.Now().Format(name+"_scan.log.2006-01-02T15.04.05Z0700"))
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("create file failed", err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	batchSize := 10240
	totalCnt := 0
	for {
		keys, values, err := rawkvClient.Scan(ctx, startKey, endKey, batchSize)
		if err != nil {
			return err
		}
		for i, key := range keys {
			totalCnt++
			msg := fmt.Sprintf("key:%s, value:%s, cnt:%d.\n",
				strings.ToUpper(hex.EncodeToString(key)), strings.ToUpper(hex.EncodeToString(values[i])),
				totalCnt)
			writer.WriteString(msg)
		}
		if len(keys) < batchSize {
			break
		}
		startKey = append(keys[len(keys)-1], 0)
	}
	writer.Flush()
	fmt.Printf("%s scan kv is written into %s.\n", name, filePath)

	return nil
}

func runRawKvScan(cmd *cobra.Command, name string) error {
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
	errGroup := new(errgroup.Group)
	errGroup.Go(func() error {
		err = RawKVScan(ctx, cfg.SrcPD, "SRC", srcAPIVersion, cfg.StartKey, cfg.EndKey)

		if err != nil {
			fmt.Printf("scan src cluster fails, %v.\n", err)
			return err
		}
		fmt.Printf("src cluster scan finish.\n")
		return nil
	})
	errGroup.Go(func() error {
		err = RawKVScan(ctx, cfg.DstPD, "DST", dstAPIVersion, cfg.StartKey, cfg.EndKey)

		if err != nil {
			fmt.Printf("Scan dst cluster fails, %v.\n", err)
			return err
		}
		fmt.Printf("dst cluster scan finish.\n")
		return nil
	})
	err = errGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func RawKVDiff(ctx context.Context, srcPdAddr, dstPdAddr string, apiVersion kvrpcpb.APIVersion, startKey, endKey []byte) error {
	srcRawkvClient, err := rawkv.NewClientWithOpts(ctx, strings.Split(srcPdAddr, ","), rawkv.WithAPIVersion(apiVersion))
	if err != nil {
		fmt.Printf("new rawkv client fails, %v.\n", err)
		return err
	}
	defer srcRawkvClient.Close()

	dstRawkvClient, err := rawkv.NewClientWithOpts(ctx, strings.Split(dstPdAddr, ","), rawkv.WithAPIVersion(apiVersion))
	if err != nil {
		fmt.Printf("new rawkv client fails, %v.\n", err)
		return err
	}
	defer dstRawkvClient.Close()

	batchSize := 10240
	totalCnt := 0
	for {
		srcKeys, srcValues, err := srcRawkvClient.Scan(ctx, startKey, endKey, batchSize)
		if err != nil {
			return err
		}
		dstKeys, dstValues, err := dstRawkvClient.Scan(ctx, startKey, endKey, batchSize)
		if err != nil {
			return err
		}
		if len(srcKeys) != len(dstKeys) {
			fmt.Printf("scan get diff key cnt, src:%d, dst:%d.", len(srcKeys), len(dstKeys))
		}
		for i, key := range srcKeys {
			totalCnt++
			if !bytes.Equal(key, dstKeys[i]) || !bytes.Equal(srcValues[i], dstValues[i]) {
				fmt.Printf("Found diff src key:%s, src value:%s, dst key:%s, dst value:%s, cnt:%d.\n",
					strings.ToUpper(hex.EncodeToString(key)), strings.ToUpper(hex.EncodeToString(srcValues[i])),
					strings.ToUpper(hex.EncodeToString(dstKeys[i])), strings.ToUpper(hex.EncodeToString(dstValues[i])),
					totalCnt)
			}
		}
		if len(srcKeys) < batchSize {
			break
		}
		startKey = append(srcKeys[len(srcKeys)-1], 0)
	}

	return nil
}

func runRawKvDiff(cmd *cobra.Command, name string) error {
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
	return RawKVDiff(ctx, cfg.SrcPD, cfg.DstPD, srcAPIVersion, cfg.StartKey, cfg.EndKey)
}

func runRawKvFormat(cmd *cobra.Command, name string) error {
	cfg := Config{}
	err := cfg.ParseFromFlags(cmd.Flags())
	if err != nil {
		fmt.Printf("parse cmd flags error, %v.\n", err)
		return err
	}
	fmt.Printf("Formated Raw StartKey:%s.\n", string(cfg.StartKey))
	fmt.Printf("Formated Raw EndKey:%s.\n", string(cfg.EndKey))
	fmt.Printf("Formated Hex StartKey:%s.\n", strings.ToUpper(hex.EncodeToString(cfg.StartKey)))
	fmt.Printf("Formated Hex EndKey:%s.\n", strings.ToUpper(hex.EncodeToString(cfg.EndKey)))
	return nil
}

func runRawKvDecodeTs(cmd *cobra.Command, name string) error {
	cfg := Config{}
	err := cfg.ParseFromFlags(cmd.Flags())
	if err != nil {
		fmt.Printf("parse cmd flags error, %v.\n", err)
		return err
	}
	ts := binary.BigEndian.Uint64(cfg.StartKey)
	fmt.Printf("Decoded Ts:%d.\n", ^ts)
	return nil
}

func NewChecksumCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "checksum",
		Short:        "commands to checksum",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvChecksum(cmd, "RawCheck")
		},
	}

	return meta
}

func NewScanCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "scan",
		Short:        "commands to checksum",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvScan(cmd, "RawScan")
		},
	}

	return meta
}

func NewDiffCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "diff",
		Short:        "commands to diff",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvDiff(cmd, "Diff")
		},
	}

	return meta
}

func NewFormatCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "format",
		Short:        "commands to format input key",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvFormat(cmd, "Format")
		},
	}

	return meta
}

func NewDecodeCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "decode",
		Short:        "commands to decode ts",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvDecodeTs(cmd, "Decode")
		},
	}

	return meta
}

func main() {
	rootCmd := &cobra.Command{
		Use:              "rawkv-data-check",
		Short:            "rawkv-data-check is a data consistency check tool for tikv raw mode.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	AddFlags(rootCmd)
	rootCmd.AddCommand(NewChecksumCommand())
	rootCmd.AddCommand(NewScanCommand())
	rootCmd.AddCommand(NewDiffCommand())
	rootCmd.AddCommand(NewFormatCommand())
	rootCmd.AddCommand(NewDecodeCommand())
	// Ouputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("rawkv check failed, %v.\n", err)
		os.Exit(1) // nolint:gocritic
	}
}
