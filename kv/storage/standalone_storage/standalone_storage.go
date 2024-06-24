package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).

	return &StandAloneStorage{
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	option := badger.DefaultOptions;
	option.Dir = s.conf.DBPath
	db, err := badger.Open(option)
	if err != nil {
		log.Fatal(err)
	}

	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return newBadgerRader(s.db)
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var txnUpdateErr error
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	for _, item := range batch {
		switch item.Data.(type){
		case storage.Put:
			key := engine_util.KeyWithCF(item.Cf(), item.Key())
			txnUpdateErr = txn.Set(key, item.Value())
			if txnUpdateErr != nil {
				break
			}
			
		case storage.Delete:
			key := engine_util.KeyWithCF(item.Cf(), item.Key())
			txnUpdateErr = txn.Delete(key)
			if txnUpdateErr != nil {
				break
			}
		default:
			panic("invalid modity type")
		}
	}

	if txnUpdateErr != nil {
		return txnUpdateErr
	}

	return txn.Commit()
}

type badgerReader struct {
	txn *badger.Txn
}

func newBadgerRader(db *badger.DB)(storage.StorageReader, error) {
	txn := db.NewTransaction(false)

	return &badgerReader{
		txn: txn,
	}, nil
}

func (r *badgerReader) Close(){
	r.txn.Discard()
	r.txn = nil
}

func(r *badgerReader) GetCF(cf string, key []byte) ([]byte, error){
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func(r *badgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
