package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	val, err := reader.GetCF(req.GetCf(), req.Key)
	if err != nil {
		return nil, err
	}

	notFound := false
	if val == nil {
		notFound = true
	}

	resp := &kvrpcpb.RawGetResponse{
		Value: val, 
		NotFound: notFound,
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	batch := []storage.Modify{
		{
		Data: storage.Put{
			Key: req.GetKey(),
			Value: req.GetValue(),
			Cf: req.GetCf(),
		},
		},
	}
	err := server.storage.Write(nil, batch)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	batch := []storage.Modify{
		{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf: req.GetCf(),
		},
		},
	}

	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	iter.Seek(req.StartKey)

	kvs := []*kvrpcpb.KvPair{}
	for {
		if len(kvs) >= int(req.GetLimit()) || ! iter.Valid() {
			break
		}

		key := iter.Item().KeyCopy(nil)
		val, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		kv := &kvrpcpb.KvPair{
			Key: key,
			Value: val,
		}
		kvs = append(kvs, kv)

		iter.Next()
	}

	resp := &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}

	return resp, nil
}
