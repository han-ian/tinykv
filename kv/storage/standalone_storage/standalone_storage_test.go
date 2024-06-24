package standalone_storage_test

import (
	"testing"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/stretchr/testify/require"
)

func TestStandAlongStorage(t *testing.T){
	conf := config.Config{
		DBPath: "/tmp/badger",
	}
	storage := standalone_storage.NewStandAloneStorage( &conf)

	storage.Start()
	defer func(){
		err := storage.Stop()
		require.NoError(t, err)
	}()


}