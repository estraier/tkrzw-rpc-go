/*************************************************************************************************
 * Iterator interface
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

package tkrzw_rpc

import (
	"context"
	"fmt"
	"time"
)

// Iterator for each record.
//
// An iterator is made by the "MakeIerator" method of RemoteDBM.  Every unused iterator object should be destructed explicitly by the "Destruct" method to free resources.
type Iterator struct {
	// Pointer to the remote database object.
	dbm *RemoteDBM
	// The stream iterface.
	stream DBMService_IterateClient
	// Function to cancel the stream.
	cancel context.CancelFunc
}

func (self *Iterator) initialize() {
	if self.dbm.conn == nil {
		self.dbm = nil
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond * time.Duration(self.dbm.timeout * 1000))
	stream, err := self.dbm.stub.Iterate(ctx)
	if err != nil {
		cancel()
		self.dbm = nil
		return
	}
	self.stream = stream
	self.cancel = cancel
}

// Releases the resource explicitly.
func (self *Iterator) Destruct() {
	if self.dbm == nil {
		return
	}
	self.cancel()
}

// Makes a string representing the iterator.
//
// @return The string representing the iterator.
func (self *Iterator) String() string {
	if self.dbm == nil {
		return fmt.Sprintf("#<tkrzw_rpc.Iterator:%p:destructed>", &self)
	}
	if self.dbm.conn == nil {
		return fmt.Sprintf("#<tkrzw_rpc.Iterator:%p:unopened>", &self)
	}
	return fmt.Sprintf("#<tkrzw_rpc.Iterator:%p:opened>", &self)
}

// Initializes the iterator to indicate the first record.
//
// @return The result status.
//
// Even if there's no record, the operation doesn't fail.
func (self *Iterator) First() *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_FIRST;
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Initializes the iterator to indicate the last record.
//
// @return The result status.
//
// Even if there's no record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
func (self *Iterator) Last() *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_LAST;
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Initializes the iterator to indicate a specific record.
//
// @param key The key of the record to look for.
// @return The result status.
//
// Ordered databases can support "lower bound" jump; If there's no record with the same key, the iterator refers to the first record whose key is greater than the given key.  The operation fails with unordered databases if there's no record with the same key.
func (self *Iterator) Jump(key interface{}) *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_JUMP;
	request.Key = ToByteArray(key)
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Initializes the iterator to indicate the last record whose key is lower than a given key.
//
// @param key The key to compare with.
// @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
// @return The result status.
//
// Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
func (self *Iterator) JumpLower(key interface{}, inclusive bool) *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_JUMP_LOWER;
	request.Key = ToByteArray(key)
	request.JumpInclusive = inclusive
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Initializes the iterator to indicate the first record whose key is upper than a given key.
//
// @param key The key to compare with.
// @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
// @return The result status.
//
// Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
func (self *Iterator) JumpUpper(key interface{}, inclusive bool) *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_JUMP_UPPER;
	request.Key = ToByteArray(key)
	request.JumpInclusive = inclusive
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Moves the iterator to the next record.
//
// @return The result status.
//
// If the current record is missing, the operation fails.  Even if there's no next record, the operation doesn't fail.
func (self *Iterator) Next() *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_NEXT;
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Moves the iterator to the previous record.
//
// @return The result status.
//
// If the current record is missing, the operation fails.  Even if there's no previous record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
func (self *Iterator) Previous() *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_PREVIOUS;
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Gets the key and the value of the current record of the iterator.
//
// @return The key and the value of the current record, and the result status.
func (self *Iterator) Get() ([]byte, []byte, *Status) {
	if self.dbm == nil {
		return nil, nil, NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return nil, nil, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_GET;
	err := self.stream.Send(&request)
	if err != nil {
		return nil, nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return nil, nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return response.Key, response.Value, makeStatusFromProto(response.Status)
	}
	return nil, nil, makeStatusFromProto(response.Status)
}

// Gets the key and the value of the current record of the iterator, as strings.
//
// @return The key and the value of the current record, and the result status.
func (self *Iterator) GetStr() (string, string, *Status) {
	if self.dbm == nil {
		return "", "", NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return "", "", NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_GET;
	err := self.stream.Send(&request)
	if err != nil {
		return "", "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return "", "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return ToString(response.Key), ToString(response.Value), makeStatusFromProto(response.Status)
	}
	return "", "", makeStatusFromProto(response.Status)
}

// Gets the key of the current record.
//
// @return The key of the current record and the result status.
func (self *Iterator) GetKey() ([]byte, *Status) {
	if self.dbm == nil {
		return nil, NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return nil, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_GET;
	request.OmitValue = true
	err := self.stream.Send(&request)
	if err != nil {
		return nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return response.Key, makeStatusFromProto(response.Status)
	}
	return nil, makeStatusFromProto(response.Status)
}

// Gets the key of the current record, as a string.
//
// @return The key of the current record and the result status.
func (self *Iterator) GetKeyStr() (string, *Status) {
	if self.dbm == nil {
		return "", NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return "", NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_GET;
	request.OmitValue = true
	err := self.stream.Send(&request)
	if err != nil {
		return "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return ToString(response.Key), makeStatusFromProto(response.Status)
	}
	return "", makeStatusFromProto(response.Status)
}

// Gets the value of the current record.
//
// @return The value of the current record and the result status.
func (self *Iterator) GetValue() ([]byte, *Status) {
	if self.dbm == nil {
		return nil, NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return nil, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_GET;
	request.OmitKey = true
	err := self.stream.Send(&request)
	if err != nil {
		return nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return response.Value, makeStatusFromProto(response.Status)
	}
	return nil, makeStatusFromProto(response.Status)
}

// Gets the value of the current record, as a string.
//
// @return The value of the current record and the result status.
func (self *Iterator) GetValueStr() (string, *Status) {
	if self.dbm == nil {
		return "", NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return "", NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_GET;
	request.OmitKey = true
	err := self.stream.Send(&request)
	if err != nil {
		return "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return ToString(response.Value), makeStatusFromProto(response.Status)
	}
	return "", makeStatusFromProto(response.Status)
}

// Sets the value of the current record.
//
// @param value The value of the record.
// @return The result status.
func (self *Iterator) Set(value interface{}) *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_SET;
	request.Value = ToByteArray(value)
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Removes the current record.
//
// @return The result status.
func (self *Iterator) Remove() *Status {
	if self.dbm == nil {
		return NewStatus2(StatusPreconditionError, "destructed iterator")
	}
	if self.dbm.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	request := IterateRequest{}
	request.DbmIndex = self.dbm.dbmIndex
	request.Operation = IterateRequest_OP_REMOVE;
	err := self.stream.Send(&request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	response, err := self.stream.Recv()
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// END OF FILE
