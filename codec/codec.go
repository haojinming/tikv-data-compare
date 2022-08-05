package codec

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"tikv-data-compare/config"

	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/util/codec"
)

func runRawKvFormat(cmd *cobra.Command, name string) error {
	cfg := config.Config{}
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
	cfg := config.Config{}
	err := cfg.ParseFromFlags(cmd.Flags())
	if err != nil {
		fmt.Printf("parse cmd flags error, %v.\n", err)
		return err
	}
	ts := binary.BigEndian.Uint64(cfg.StartKey)
	fmt.Printf("Decoded Ts:%d.\n", ^ts)
	return nil
}

func runRawKvEncodeKey(cmd *cobra.Command, name string) error {
	cfg := config.Config{}
	err := cfg.ParseFromFlags(cmd.Flags())
	if err != nil {
		fmt.Printf("parse cmd flags error, %v.\n", err)
		return err
	}
	apiv2Prefix := []byte{'r', 0, 0, 0}
	startKey := append(apiv2Prefix, cfg.StartKey...)
	endKey := append(apiv2Prefix, cfg.EndKey...)
	codec.EncodeBytes(nil, startKey)
	fmt.Printf("Encoded StartKey:%s.\n", strings.ToUpper(hex.EncodeToString(codec.EncodeBytes(nil, startKey))))
	fmt.Printf("Encoded EndKey:%s.\n", strings.ToUpper(hex.EncodeToString(codec.EncodeBytes(nil, endKey))))
	return nil
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

func NewDecodeTsCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "decodets",
		Short:        "commands to decode ts",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvDecodeTs(cmd, "Decode")
		},
	}

	return meta
}

func NewEncodeKeyCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:          "encodekey",
		Short:        "commands to encode apiv2 key",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRawKvEncodeKey(cmd, "Encode Key")
		},
	}

	return meta
}
