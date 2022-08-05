package config

import (
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/tikv/migration/br/pkg/utils"
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
