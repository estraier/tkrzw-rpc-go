/*************************************************************************************************
 * Example for basic usage of the remote database
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

package main

import (
	"fmt"
	"github.com/estraier/tkrzw-rpc-go"
)

func main() {
	// Prepares the database.
	// The timeout is in seconds.
	// The method OrDie causes panic if the status is not success.
	// You should write your own error handling in large scale programs.
	dbm := tkrzw_rpc.NewRemoteDBM()
	dbm.Connect("localhost:1978", 10, "").OrDie()

	// Closes the connection for sure and checks the error too.
	defer func() { dbm.Disconnect().OrDie() }()

	// Two bank accounts for Bob and Alice.
	// Numeric values are converted into strings implicitly.
	dbm.Set("Bob", 1000, false).OrDie()
	dbm.Set("Alice", 3000, false).OrDie()

	// Function to do a money transfer atomically.
	transfer := func(src_key string, dest_key string, amount int64) *tkrzw_rpc.Status {
		// Gets the old values as numbers.
		old_src_value := tkrzw_rpc.ToInt(dbm.GetStrSimple(src_key, "0"))
		old_dest_value := tkrzw_rpc.ToInt(dbm.GetStrSimple(dest_key, "0"))

		// Calculates the new values.
		new_src_value := old_src_value - amount
		new_dest_value := old_dest_value + amount
		if new_src_value < 0 {
			return tkrzw_rpc.NewStatus(tkrzw_rpc.StatusApplicationError, "insufficient value")
		}

		// Prepares the pre-condition and the post-condition of the transaction.
		old_records := []tkrzw_rpc.KeyValueStrPair{
			{src_key, tkrzw_rpc.ToString(old_src_value)},
			{dest_key, tkrzw_rpc.ToString(old_dest_value)},
		}
		new_records := []tkrzw_rpc.KeyValueStrPair{
			{src_key, tkrzw_rpc.ToString(new_src_value)},
			{dest_key, tkrzw_rpc.ToString(new_dest_value)},
		}

		// Performs the transaction atomically.
		// This fails safely if other concurrent transactions break the pre-condition.
		return dbm.CompareExchangeMultiStr(old_records, new_records)
	}

	// Tries a transaction until it succeeds
	var status *tkrzw_rpc.Status
	for num_tries := 0; num_tries < 100; num_tries++ {
		status = transfer("Alice", "Bob", 500)
		if !status.Equals(tkrzw_rpc.StatusInfeasibleError) {
			break
		}
	}
	status.OrDie()

	// Traverses records in a primitive way.
	iter := dbm.MakeIterator()
	defer iter.Destruct()
	iter.First()
	for {
		key, value, status := iter.GetStr()
		if !status.IsOK() {
			break
		}
		fmt.Println(key, value)
		iter.Next()
	}
}

// END OF FILE
