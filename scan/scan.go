package scan

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"tikv-data-compare/config"
	"tikv-data-compare/utils"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/rawkv"
	"golang.org/x/sync/errgroup"
)

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
