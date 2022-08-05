package checksum

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"tikv-data-compare/config"
	"tikv-data-compare/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/util/codec"
	"github.com/tikv/migration/br/pkg/gluetikv"
	"github.com/tikv/migration/br/pkg/pdutil"
	"github.com/tikv/migration/br/pkg/restore"
	brutils "github.com/tikv/migration/br/pkg/utils"
	pd "github.com/tikv/pd/client"
	"golang.org/x/sync/errgroup"
)

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
	keyRanges := []*brutils.KeyRange{}
	for _, region := range regionInfo {
		keyRange := brutils.KeyRange{
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
	workerPool := brutils.NewWorkerPool(concurrency, "Ranges")
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
	cfg := config.Config{}
	err := cfg.ParseFromFlags(cmd.Flags())
	if err != nil {
		fmt.Printf("parse cmd flags error, %v.\n", err)
		return err
	}
	ctx := context.Background()
	srcAPIVersion, err := utils.GetClusterAPIVersion(ctx, cfg.SrcPD)
	if err != nil {
		return err
	}
	dstAPIVersion, err := utils.GetClusterAPIVersion(ctx, cfg.DstPD)
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
