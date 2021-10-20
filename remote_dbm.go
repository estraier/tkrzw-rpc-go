/*************************************************************************************************
 * Remote database manager
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"time"
	"unsafe"
)

func strGRPCError(err error) string {
	status := status.Convert(err)
	return status.Code().String() + ": " + status.Message()
}

func makeStatusFromProto(proto_status *StatusProto) *Status {
	if len(proto_status.Message) == 0 {
		return NewStatus1(StatusCode(proto_status.Code))
	}
	return NewStatus2(StatusCode(proto_status.Code), proto_status.Message)
}

// Remote database manager.
//
// All operations except for "Connect" and "Disconnect" are thread-safe; Multiple threads can access the same database concurrently.  The "SetDBMIndex" affects all threads so it should be called before the object is shared.
type RemoteDBM struct {
	conn     *grpc.ClientConn
	stub     DBMServiceClient
	timeout  float64
	dbmIndex int32
}

// A pair of the key and the value of a record.
type KeyValuePair struct {
	// The key.
	Key []byte
	// The value
	Value []byte
}

// A string pair of the key and the value of a record.
type KeyValueStrPair struct {
	// The key.
	Key string
	// The value
	Value string
}

// Makes a new RemoteDBM object.
//
// @return The pointer to the created remote database object.
func NewRemoteDBM() *RemoteDBM {
	return &RemoteDBM{nil, nil, 0, 0}
}

// Makes a string representing the remote database.
//
// @return The string representing the remote database.
func (self *RemoteDBM) String() string {
	if self.conn == nil {
		return fmt.Sprintf("#<tkrzw_rpc.RemoteDBM:%p:unopened>", &self)
	}
	return fmt.Sprintf("#<tkrzw_rpc.RemoteDBM:%p:opened>", &self)
}

// Connects to the server.
//
// @param address The address or the host name of the server and its port number.  For IPv4 address, it's like "127.0.0.1:1978".  For IPv6, it's like "[::1]:1978".  For UNIX domain sockets, it's like "unix:/path/to/file".
// @param timeout The timeout in seconds for connection and each operation.  Negative means unlimited.
// @return The result status.
func (self *RemoteDBM) Connect(address string, timeout float64) *Status {
	if self.conn != nil {
		return NewStatus2(StatusPreconditionError, "opened connection")
	}
	if timeout < 0 {
		timeout = float64(1 << 30)
	}
	deadline := time.Now().Add(time.Millisecond * time.Duration(timeout*1000))
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	maxFailures := 3
	numFailures := 0
	for {
		if time.Now().After(deadline) {
			return NewStatus2(StatusNetworkError, "connection timeout")
		}
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if state == connectivity.Idle && numFailures > 0 {
			numFailures += 1
		} else if state == connectivity.TransientFailure {
			numFailures += 1
		} else if state == connectivity.Shutdown {
			numFailures = maxFailures
		}
		if numFailures >= maxFailures {
			conn.Close()
			return NewStatus2(StatusNetworkError, "connection failed")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		conn.WaitForStateChange(ctx, state)
	}
	self.conn = conn
	self.stub = NewDBMServiceClient(conn)
	self.timeout = timeout
	self.dbmIndex = 0
	return NewStatus1(StatusSuccess)
}

// Disconnects the connection to the server.
//
// @return The result status.
func (self *RemoteDBM) Disconnect() *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	status := NewStatus1(StatusSuccess)
	err := self.conn.Close()
	if err != nil {
		status.Set(StatusNetworkError, strGRPCError(err))
	}
	self.conn = nil
	self.stub = nil
	return status
}

// Sets the index of the DBM to access.
//
// @param dbmIndex The index of the DBM to access.
// @eturn The result status.
func (self *RemoteDBM) SetDBMIndex(dbmIndex int32) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	self.dbmIndex = dbmIndex
	return NewStatus1(StatusSuccess)
}

// Sends a message and gets back the echo message.
//
// @param message The message to send.
// @param status A status object to which the result status is assigned.  It can be omitted.
// @return The string value of the echoed message or None on failure.
func (self *RemoteDBM) Echo(message string) (string, *Status) {
	if self.conn == nil {
		return "", NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := EchoRequest{}
	request.Message = message
	response, err := self.stub.Echo(ctx, &request)
	if err != nil {
		return "", NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return response.Echo, NewStatus1(StatusSuccess)
}

// Inspects the database.
//
// @return A map of property names and their values.
//
// If the DBM index is negative, basic metadata of all DBMs are obtained.
func (self *RemoteDBM) Inspect() map[string]string {
	result := make(map[string]string)
	if self.conn == nil {
		return result
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := InspectRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.Inspect(ctx, &request)
	if err != nil {
		return result
	}
	for _, record := range response.Records {
		result[record.First] = record.Second
	}
	return result
}

// Gets the value of a record of a key.
//
// @param key The key of the record.
// @return The bytes value of the matching record and the result status.  If there's no matching record, the status is StatusNotFoundError.
func (self *RemoteDBM) Get(key interface{}) ([]byte, *Status) {
	if self.conn == nil {
		return nil, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := GetRequest{}
	request.DbmIndex = self.dbmIndex
	request.Key = ToByteArray(key)
	response, err := self.stub.Get(ctx, &request)
	if err != nil {
		return nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return response.Value, makeStatusFromProto(response.Status)
	}
	return nil, makeStatusFromProto(response.Status)
}

// Gets the value of a record of a key, as a string.
//
// @param key The key of the record.
// @return The string value of the matching record and the result status.  If there's no matching record, the status is StatusNotFoundError.
func (self *RemoteDBM) GetStr(key interface{}) (string, *Status) {
	value, status := self.Get(key)
	if status.GetCode() == StatusSuccess {
		return *(*string)(unsafe.Pointer(&value)), status
	}
	return "", status
}

// Gets the value of a record of a key, in a simple way.
//
// @param key The key of the record.
// @param defaultValue The value to be returned on failure.
// @return The value of the matching record on success, or the default value on failure.
func (self *RemoteDBM) GetSimple(key interface{}, defaultValue interface{}) []byte {
	value, status := self.Get(key)
	if status.GetCode() == StatusSuccess {
		return value
	}
	return ToByteArray(defaultValue)
}

// Gets the value of a record of a key, in a simple way, as a string.
//
// @param key The key of the record.
// @param defaultValue The value to be returned on failure.
// @return The value of the matching record on success, or the default value on failure.
func (self *RemoteDBM) GetStrSimple(key interface{}, defaultValue interface{}) string {
	value, status := self.GetStr(key)
	if status.GetCode() == StatusSuccess {
		return value
	}
	return ToString(defaultValue)
}

// Gets the values of multiple records of keys.
//
// @param keys The keys of records to retrieve.
// @return A map of retrieved records.  Keys which don't match existing records are ignored.
func (self *RemoteDBM) GetMulti(keys []string) map[string][]byte {
	result := make(map[string][]byte)
	if self.conn == nil {
		return result
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := GetMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for _, key := range keys {
		request.Keys = append(request.Keys, ToByteArray(key))
	}
	response, err := self.stub.GetMulti(ctx, &request)
	if err != nil {
		return result
	}
	for _, record := range response.Records {
		result[ToString(record.First)] = record.Second
	}
	return result
}

// Gets the values of multiple records of keys, as strings.
//
// @param keys The keys of records to retrieve.
// @eturn A map of retrieved records.  Keys which don't match existing records are ignored.
func (self *RemoteDBM) GetMultiStr(keys []string) map[string]string {
	result := make(map[string]string)
	if self.conn == nil {
		return result
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := GetMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for _, key := range keys {
		request.Keys = append(request.Keys, ToByteArray(key))
	}
	response, err := self.stub.GetMulti(ctx, &request)
	if err != nil {
		return result
	}
	for _, record := range response.Records {
		result[ToString(record.First)] = ToString(record.Second)
	}
	return result
}

// Sets a record of a key and a value.
//
// @param key The key of the record.
// @param value The value of the record.
// @param overwrite Whether to overwrite the existing value.
// @return The result status.  If overwriting is abandoned, StatusDuplicationError is returned.
func (self *RemoteDBM) Set(key interface{}, value interface{}, overwrite bool) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := SetRequest{}
	request.DbmIndex = self.dbmIndex
	request.Key = ToByteArray(key)
	request.Value = ToByteArray(value)
	request.Overwrite = overwrite
	response, err := self.stub.Set(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Sets multiple records.
//
// @param records Records to store.
// @param overwrite Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
// @return The result status.  If there are records avoiding overwriting, StatusDuplicationError is returned.
func (self *RemoteDBM) SetMulti(records map[string][]byte, overwrite bool) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := SetMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for key, value := range records {
		record := BytesPair{}
		record.First = ToByteArray(key)
		record.Second = value
		request.Records = append(request.Records, &record)
	}
	request.Overwrite = overwrite
	response, err := self.stub.SetMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Sets multiple records, with string data.
//
// @param records Records to store.
// @param overwrite Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
// @return The result status.  If there are records avoiding overwriting, StatusDuplicationError is set.
func (self *RemoteDBM) SetMultiStr(records map[string]string, overwrite bool) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := SetMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for key, value := range records {
		record := BytesPair{}
		record.First = ToByteArray(key)
		record.Second = ToByteArray(value)
		request.Records = append(request.Records, &record)
	}
	request.Overwrite = overwrite
	response, err := self.stub.SetMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Removes a record of a key.
//
// @param key The key of the record.
// @return The result status.  If there's no matching record, StatusNotFoundError is returned.
func (self *RemoteDBM) Remove(key interface{}) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := RemoveRequest{}
	request.DbmIndex = self.dbmIndex
	request.Key = ToByteArray(key)
	response, err := self.stub.Remove(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Removes records of keys.
//
// @param key The keys of the records.
// @return The result status.  If there are missing records, StatusNotFoundError is returned.
func (self *RemoteDBM) RemoveMulti(keys []string) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := RemoveMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for _, key := range keys {
		request.Keys = append(request.Keys, ToByteArray(key))
	}
	response, err := self.stub.RemoveMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Appends data at the end of a record of a key.
//
// @param key The key of the record.
// @param value The value to append.
// @param delim The delimiter to put after the existing record.
// @return The result status.
//
// If there's no existing record, the value is set without the delimiter.
func (self *RemoteDBM) Append(key interface{}, value interface{}, delim interface{}) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := AppendRequest{}
	request.DbmIndex = self.dbmIndex
	request.Key = ToByteArray(key)
	request.Value = ToByteArray(value)
	request.Delim = ToByteArray(delim)
	response, err := self.stub.Append(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Appends data to multiple records.
//
// @param records Records to append.
// @param delim The delimiter to put after the existing record.
// @return The result status.
//
// If there's no existing record, the value is set without the delimiter.
func (self *RemoteDBM) AppendMulti(records map[string][]byte, delim interface{}) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := AppendMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for key, value := range records {
		record := BytesPair{}
		record.First = ToByteArray(key)
		record.Second = value
		request.Records = append(request.Records, &record)
	}
	request.Delim = ToByteArray(delim)
	response, err := self.stub.AppendMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Appends data to multiple records, with string data.
//
// @param records Records to append.
// @param delim The delimiter to put after the existing record.
// @return The result status.
//
// If there's no existing record, the value is set without the delimiter.
func (self *RemoteDBM) AppendMultiStr(records map[string]string, delim interface{}) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := AppendMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for key, value := range records {
		record := BytesPair{}
		record.First = ToByteArray(key)
		record.Second = ToByteArray(value)
		request.Records = append(request.Records, &record)
	}
	request.Delim = ToByteArray(delim)
	response, err := self.stub.AppendMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Compares the value of a record and exchanges if the condition meets.
//
// @param key The key of the record.
// @param expected The expected value.  If it is nil, no existing record is expected.
// @param desired The desired value.  If it is nil, the record is to be removed.
// @return The result status.  If the condition doesn't meet, StatusInfeasibleError is returned.
func (self *RemoteDBM) CompareExchange(
	key interface{}, expected interface{}, desired interface{}) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := CompareExchangeRequest{}
	request.DbmIndex = self.dbmIndex
	request.Key = ToByteArray(key)
	if expected != nil {
		request.ExpectedExistence = true
		request.ExpectedValue = ToByteArray(expected)
	}
	if desired != nil {
		request.DesiredExistence = true
		request.DesiredValue = ToByteArray(desired)
	}
	response, err := self.stub.CompareExchange(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Increments the numeric value of a record.
//
// @param key The key of the record.
// @param inc The incremental value.  If it is Int64Min, the current value is not changed and a new record is not created.
// @param init The initial value.
// @return The current value and the result status.
func (self *RemoteDBM) Increment(
	key interface{}, inc interface{}, init interface{}) (int64, *Status) {
	if self.conn == nil {
		return 0, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := IncrementRequest{}
	request.DbmIndex = self.dbmIndex
	request.Key = ToByteArray(key)
	request.Increment = ToInt(inc)
	request.Initial = ToInt(init)
	response, err := self.stub.Increment(ctx, &request)
	if err != nil {
		return 0, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return response.Current, makeStatusFromProto(response.Status)
}

// Compares the values of records and exchanges if the condition meets.
//
// @param expected A sequence of pairs of the record keys and their expected values.  If the value is nil, no existing record is expected.
// @param desired A sequence of pairs of the record keys and their desired values.  If the value is nil, the record is to be removed.
// @return The result status.  If the condition doesn't meet, StatusInfeasibleError is returned.
func (self *RemoteDBM) CompareExchangeMulti(
	expected []KeyValuePair, desired []KeyValuePair) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := CompareExchangeMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for _, record := range expected {
		state := RecordState{}
		state.Key = record.Key
		if record.Value != nil {
			state.Existence = true
			state.Value = record.Value
		}
		request.Expected = append(request.Expected, &state)
	}
	for _, record := range desired {
		state := RecordState{}
		state.Key = record.Key
		if record.Value != nil {
			state.Existence = true
			state.Value = record.Value
		}
		request.Desired = append(request.Desired, &state)
	}
	response, err := self.stub.CompareExchangeMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Compares the values of records and exchanges if the condition meets, using string data.
//
// @param expected A sequence of pairs of the record keys and their expected values.  If the value is an empty string, no existing record is expected.
// @param desired A sequence of pairs of the record keys and their desired values.  If the value is an empty string, the record is to be removed.
// @return The result status.  If the condition doesn't meet, StatusInfeasibleError is returned.
func (self *RemoteDBM) CompareExchangeMultiStr(
	expected []KeyValueStrPair, desired []KeyValueStrPair) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := CompareExchangeMultiRequest{}
	request.DbmIndex = self.dbmIndex
	for _, record := range expected {
		state := RecordState{}
		state.Key = ToByteArray(record.Key)
		if len(record.Value) != 0 {
			state.Existence = true
			state.Value = ToByteArray(record.Value)
		}
		request.Expected = append(request.Expected, &state)
	}
	for _, record := range desired {
		state := RecordState{}
		state.Key = ToByteArray(record.Key)
		if len(record.Value) != 0 {
			state.Existence = true
			state.Value = ToByteArray(record.Value)
		}
		request.Desired = append(request.Desired, &state)
	}
	response, err := self.stub.CompareExchangeMulti(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Changes the key of a record.
//
// @param oldKey The old key of the record.
// @param newKey The new key of the record.
// @param overwrite Whether to overwrite the existing record of the new key.
// @param copying Whether to retain the record of the old key.
// @return The result status.  If there's no matching record to the old key, NOT_FOUND_ERROR is returned.  If the overwrite flag is false and there is an existing record of the new key, DUPLICATION ERROR is returned.
//
// This method is done atomically by ProcessMulti.  The other threads observe that the record has either the old key or the new key.  No intermediate states are observed.
func (self *RemoteDBM) Rekey(oldKey interface{}, newKey interface{},
	overwrite bool, copying bool) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := RekeyRequest{}
	request.DbmIndex = self.dbmIndex
	request.OldKey = ToByteArray(oldKey)
	request.NewKey = ToByteArray(newKey)
	request.Overwrite = overwrite
	request.Copying = copying
	response, err := self.stub.Rekey(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Gets the first record and removes it.
//
// @param retryWait The maximum wait time in seconds before retrying.  If it is zero, no retry is done.  If it is positive, retry is done and wait for the notifications of the next update for the time at most.
// @return The key and the value of the first record, and the result status.
func (self *RemoteDBM) PopFirst(retryWait float64) ([]byte, []byte, *Status) {
	if self.conn == nil {
		return nil, nil, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := PopFirstRequest{}
	request.DbmIndex = self.dbmIndex
	request.RetryWait = retryWait
	response, err := self.stub.PopFirst(ctx, &request)
	if err != nil {
		return nil, nil, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	if StatusCode(response.Status.Code) == StatusSuccess {
		return response.Key, response.Value, makeStatusFromProto(response.Status)
	}
	return nil, nil, makeStatusFromProto(response.Status)
}

// Gets the first record as strings and removes it.
//
// @param retryWait The maximum wait time in seconds before retrying.  If it is zero, no retry is done.  If it is positive, retry is done and wait for the notifications of the next update for the time at most.
// @return The key and the value of the first record, and the result status.
func (self *RemoteDBM) PopFirstStr(retryWait float64) (string, string, *Status) {
	key, value, status := self.PopFirst(retryWait)
	if status.GetCode() == StatusSuccess {
		return *(*string)(unsafe.Pointer(&key)), *(*string)(unsafe.Pointer(&value)), status
	}
	return "", "", status
}

// Adds a record with a key of the current timestamp.
//
// @param value The value of the record.
// @param wtime The current wall time used to generate the key.  If it is None, the system clock is used.
// @return The result status.
//
// The key is generated as an 8-bite big-endian binary string of the timestamp.  If there is an existing record matching the generated key, the key is regenerated and the attempt is repeated until it succeeds.
func (self *RemoteDBM) PushLast(value interface{}, wtime float64) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := PushLastRequest{}
	request.DbmIndex = self.dbmIndex
	request.Value = ToByteArray(value)
	request.Wtime = wtime
	response, err := self.stub.PushLast(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Gets the number of records.
//
// @return The number of records and the result status.
func (self *RemoteDBM) Count() (int64, *Status) {
	if self.conn == nil {
		return 0, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := CountRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.Count(ctx, &request)
	if err != nil {
		return 0, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return response.Count, makeStatusFromProto(response.Status)
}

// Gets the number of records, in a simple way.
//
// @return The number of records or -1 on failure.
func (self *RemoteDBM) CountSimple() int64 {
	if self.conn == nil {
		return -1
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := CountRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.Count(ctx, &request)
	if err != nil {
		return -1
	}
	if response.Status.Code == 0 {
		return response.Count
	}
	return -1
}

// Gets the current file size of the database.
//
// @return The current file size of the database and the result status.
func (self *RemoteDBM) GetFileSize() (int64, *Status) {
	if self.conn == nil {
		return 0, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := GetFileSizeRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.GetFileSize(ctx, &request)
	if err != nil {
		return 0, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return response.FileSize, makeStatusFromProto(response.Status)
}

// Gets the current file size of the database, in a simple way.
//
// @return The current file size of the database, or -1 on failure.
func (self *RemoteDBM) GetFileSizeSimple() int64 {
	if self.conn == nil {
		return -1
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := GetFileSizeRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.GetFileSize(ctx, &request)
	if err != nil {
		return -1
	}
	if response.Status.Code == 0 {
		return response.FileSize
	}
	return -1
}

// Removes all records.
//
// @return The result status.
func (self *RemoteDBM) Clear() *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := ClearRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.Clear(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Rebuilds the entire database.
//
// @param params Optional parameters.  If it is nil, it is ignored.
// @return The result status.
//
// The optional parameters are the same as the Open method of the local DBM class and the database configurations of the server command.  Omitted tuning parameters are kept the same or implicitly optimized.
//
// In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
//
// - skip_broken_records (bool): If true, the operation continues even if there are broken records which can be skipped.
// - sync_hard (bool): If true, physical synchronization with the hardware is done before finishing the rebuilt file.
func (self *RemoteDBM) Rebuild(params map[string]string) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := RebuildRequest{}
	request.DbmIndex = self.dbmIndex
	if params != nil {
		for key, value := range params {
			param := StringPair{}
			param.First = key
			param.Second = value
			request.Params = append(request.Params, &param)
		}
	}
	response, err := self.stub.Rebuild(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Checks whether the database should be rebuilt.
//
// @return The result decision and the result status.  The decision is true to be optimized or false with no necessity.
func (self *RemoteDBM) ShouldBeRebuilt() (bool, *Status) {
	if self.conn == nil {
		return false, NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := ShouldBeRebuiltRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.ShouldBeRebuilt(ctx, &request)
	if err != nil {
		return false, NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return response.Tobe, makeStatusFromProto(response.Status)
}

// Checks whether the database should be rebuilt, in a simple way.
//
// @return True to be optimized or false with no necessity.
func (self *RemoteDBM) ShouldBeRebuiltSimple() bool {
	if self.conn == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := ShouldBeRebuiltRequest{}
	request.DbmIndex = self.dbmIndex
	response, err := self.stub.ShouldBeRebuilt(ctx, &request)
	if err != nil {
		return false
	}
	if response.Status.Code == 0 {
		return response.Tobe
	}
	return false
}

// Synchronizes the content of the database to the file system.
//
// @param hard True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
// @param params Optional parameters.  If it is nil, it is ignored.
// @return The result status.
//
// The "reducer" parameter specifies the reducer for SkipDBM.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast", etc are supported.  If the parameter "make_backup" exists, a backup file is created in the same directory as the database file.  The backup file name has a date suffix in GMT, like ".backup.20210831213749".  If the value of "make_backup" not empty, it is the value is used as the suffix.
func (self *RemoteDBM) Synchronize(hard bool, params map[string]string) *Status {
	if self.conn == nil {
		return NewStatus2(StatusPreconditionError, "not opened connection")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := SynchronizeRequest{}
	request.DbmIndex = self.dbmIndex
	request.Hard = hard
	if params != nil {
		for key, value := range params {
			param := StringPair{}
			param.First = key
			param.Second = value
			request.Params = append(request.Params, &param)
		}
	}
	response, err := self.stub.Synchronize(ctx, &request)
	if err != nil {
		return NewStatus2(StatusNetworkError, strGRPCError(err))
	}
	return makeStatusFromProto(response.Status)
}

// Searches the database and get keys which match a pattern.
//
// @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts keys whose edit distance to the binary pattern is the least.
// @param pattern The pattern for matching.
// @param capacity The maximum records to obtain.  0 means unlimited.
// @return A list of keys matching the condition.
func (self *RemoteDBM) Search(mode string, pattern string, capacity int) []string {
	result := make([]string, 0, 16)
	if self.conn == nil {
		return result
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), time.Millisecond*time.Duration(self.timeout*1000))
	defer cancel()
	request := SearchRequest{}
	request.DbmIndex = self.dbmIndex
	request.Mode = mode
	request.Pattern = ToByteArray(pattern)
	request.Capacity = int32(capacity)
	response, err := self.stub.Search(ctx, &request)
	if err != nil {
		return result
	}
	for _, key := range response.Matched {
		result = append(result, ToString(key))
	}
	return result
}

// Makes an iterator for each record.
//
// @return The iterator for each record.
//
// Every iterator should be destructed explicitly by the "Destruct" method.
func (self *RemoteDBM) MakeIterator() *Iterator {
	iter := &Iterator{self, nil, nil}
	iter.initialize()
	return iter
}

// Makes a channel to read each records.
//
// @return the channel to read each records.  All values should be read from the channel to avoid resource leak.
func (self *RemoteDBM) Each() <-chan KeyValuePair {
	chan_record := make(chan KeyValuePair)
	reader := func(chan_send chan<- KeyValuePair) {
		defer close(chan_record)
		iter := self.MakeIterator()
		defer iter.Destruct()
		if !iter.First().IsOK() {
			return
		}
		for {
			key, value, status := iter.Get()
			if !status.IsOK() {
				break
			}
			chan_send <- KeyValuePair{key, value}
			if !iter.Next().IsOK() {
				return
			}
		}
	}
	go reader(chan_record)
	return chan_record
}

// Makes a channel to read each records, as strings.
//
// @return the channel to read each records.  All values should be read from the channel to avoid resource leak.
func (self *RemoteDBM) EachStr() <-chan KeyValueStrPair {
	chan_record := make(chan KeyValueStrPair)
	reader := func(chan_send chan<- KeyValueStrPair) {
		defer close(chan_record)
		iter := self.MakeIterator()
		defer iter.Destruct()
		if !iter.First().IsOK() {
			return
		}
		for {
			key, value, status := iter.GetStr()
			if !status.IsOK() {
				break
			}
			chan_send <- KeyValueStrPair{key, value}
			if !iter.Next().IsOK() {
				return
			}
		}
	}
	go reader(chan_record)
	return chan_record
}

// END OF FILE
