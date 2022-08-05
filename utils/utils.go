package utils

import (
	"context"
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/migration/br/pkg/conn"
	"github.com/tikv/migration/br/pkg/pdutil"
	pd "github.com/tikv/pd/client"
)

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
