package diff

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"tikv-data-compare/config"
	"tikv-data-compare/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
)

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
	return RawKVDiff(ctx, cfg.SrcPD, cfg.DstPD, srcAPIVersion, cfg.StartKey, cfg.EndKey)
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
