/*************************************************************************************************
 * Test cases
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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func CheckEq(t *testing.T, want interface{}, got interface{}) {
	_, _, line, _ := runtime.Caller(1)
	if want == nil {
		if got != nil {
			t.Errorf("line=%d: not equal: want=%q, got=%q", line, want, got)
		}
		return
	}
	switch want := want.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
		if ToInt(want) != ToInt(got) {
			t.Errorf("line=%d: not equal: want=%d, got=%d", line, want, got)
		}
	case float32, float64, complex64, complex128:
		if ToFloat(want) != ToFloat(got) {
			t.Errorf("line=%d: not equal: want=%d, got=%d", line, want, got)
		}
	case string:
		if want != ToString(got) {
			t.Errorf("line=%d: not equal: want=%s, got=%s", line, want, got)
		}
	case []byte:
		if !reflect.DeepEqual(want, ToByteArray(got)) {
			t.Errorf("line=%d: not equal: want=%q, got=%q", line, want, got)
		}
	case Status:
		if !want.Equals(got) {
			t.Errorf("line=%d: not equal: want=%s, got=%s", line, want.String(), got)
		}
	case *Status:
		if !want.Equals(got) {
			t.Errorf("line=%d: not equal: want=%s, got=%s", line, want.String(), got)
		}
	case StatusCode:
		switch got := got.(type) {
		case Status:
			if !got.Equals(want) {
				t.Errorf("line=%d: not equal: want=%s, got=%s", line, StatusCodeName(want), got.String())
			}
		case *Status:
			if !got.Equals(want) {
				t.Errorf("line=%d: not equal: want=%s, got=%s", line, StatusCodeName(want), got.String())
			}
		case StatusCode:
			if want != got {
				t.Errorf("line=%d: not equal: want=%s, got=%s", line, StatusCodeName(want), StatusCodeName(got))
			}
		default:
			t.Errorf("line=%d: not comparable: want=%s, got=%q", line, StatusCodeName(want), got)
		}
	default:
		if want != got {
			t.Errorf("line=%d: not equal: want=%q, got=%q", line, want, got)
		}
	}
}

func CheckNe(t *testing.T, want interface{}, got interface{}) {
	_, _, line, _ := runtime.Caller(1)
	if want == nil {
		if got == nil {
			t.Errorf("line=%d: equal: want=%q, got=%q", line, want, got)
		}
		return
	}
	switch want := want.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
		if ToInt(want) == ToInt(got) {
			t.Errorf("line=%d: equal: want=%d, got=%d", line, want, got)
		}
	case float32, float64, complex64, complex128:
		if ToFloat(want) == ToFloat(got) {
			t.Errorf("line=%d: equal: want=%d, got=%d", line, want, got)
		}
	case string:
		if want == ToString(got) {
			t.Errorf("line=%d: equal: want=%s, got=%s", line, want, got)
		}
	case []byte:
		if reflect.DeepEqual(want, ToByteArray(got)) {
			t.Errorf("line=%d: equal: want=%q, got=%q", line, want, got)
		}
	case Status:
		if want.Equals(got) {
			t.Errorf("line=%d: equal: want=%s, got=%s", line, want.String(), got)
		}
	case *Status:
		if want.Equals(got) {
			t.Errorf("line=%d: equal: want=%s, got=%s", line, want.String(), got)
		}
	case StatusCode:
		switch got := got.(type) {
		case Status:
			if got.Equals(want) {
				t.Errorf("line=%d: equal: want=%s, got=%s", line, StatusCodeName(want), got.String())
			}
		case *Status:
			if got.Equals(want) {
				t.Errorf("line=%d: equal: want=%s, got=%s", line, StatusCodeName(want), got.String())
			}
		case StatusCode:
			if want == got {
				t.Errorf("line=%d: equal: want=%s, got=%s", line, StatusCodeName(want), StatusCodeName(got))
			}
		default:
			t.Errorf("line=%d: not comparable: want=%s, got=%q", line, StatusCodeName(want), got)
		}
	default:
		if want == got {
			t.Errorf("line=%d: equal: want=%q, got=%q", line, want, got)
		}
	}
}

func CheckTrue(t *testing.T, got bool) {
	_, _, line, _ := runtime.Caller(1)
	if !got {
		t.Errorf("line=%d: not true", line)
	}
}

func CheckFalse(t *testing.T, got bool) {
	_, _, line, _ := runtime.Caller(1)
	if got {
		t.Errorf("line=%d: true", line)
	}
}

func MakeTempDir() string {
	tmpPath := path.Join(os.TempDir(), fmt.Sprintf(
		"tkrzw-test-%04x%08x", os.Getpid()%(1<<16), time.Now().Unix()%(1<<32)))
	error := os.MkdirAll(tmpPath, 0755)
	if error != nil {
		panic(fmt.Sprintf("cannot create directory: %s", error))
	}
	return tmpPath
}

func TestAssertion(t *testing.T) {
	CheckEq(t, nil, nil)
	CheckNe(t, nil, 0)
	CheckEq(t, 2, 2)
	CheckEq(t, 2.0, 2.0)
	CheckEq(t, "two", "two")
	CheckEq(t, []byte("two"), []byte("two"))
	CheckEq(t, nil, nil)
	CheckEq(t, 2, 2.0)
	CheckEq(t, 2, "2")
	CheckEq(t, 2.0, 2)
	CheckEq(t, 2.0, "2")
	CheckEq(t, "2", 2)
	CheckEq(t, []byte("2"), 2)
	CheckNe(t, 2, 3)
	CheckNe(t, 2.0, 3.0)
	CheckNe(t, "two", "three")
	CheckNe(t, []byte("two"), []byte("three"))
	CheckNe(t, nil, 0)
	CheckTrue(t, true)
	CheckTrue(t, 1 > 0)
	CheckFalse(t, false)
	CheckFalse(t, 1 < 0)
}

type Person struct {
	Name string
}

func (self Person) String() string {
	return fmt.Sprintf("I'm %s.", self.Name)
}

func TestToString(t *testing.T) {
	CheckEq(t, "123", ToString("123"))
	CheckEq(t, "123", ToString([]byte("123")))
	CheckEq(t, "123", ToString(123))
	CheckEq(t, "123.000000", ToString(123.0))
	CheckEq(t, "true", ToString(true))
	CheckEq(t, "false", ToString(false))
	CheckEq(t, "Boom", ToString(errors.New("Boom")))
	CheckEq(t, "I'm Alice.", ToString(Person{"Alice"}))
	CheckEq(t, "I'm Bob.", ToString(&Person{"Bob"}))
}

func TestToByteArray(t *testing.T) {
	CheckEq(t, []byte("123"), ToByteArray("123"))
	CheckEq(t, []byte("123"), ToByteArray([]byte("123")))
	CheckEq(t, []byte("123"), ToByteArray(123))
	CheckEq(t, []byte("123.000000"), ToByteArray(123.0))
	CheckEq(t, []byte("true"), ToByteArray(true))
	CheckEq(t, []byte("false"), ToByteArray(false))
	CheckEq(t, []byte("Boom"), ToByteArray(errors.New("Boom")))
	CheckEq(t, []byte("I'm Alice."), ToByteArray(Person{"Alice"}))
	CheckEq(t, []byte("I'm Bob."), ToByteArray(&Person{"Bob"}))
}

func TestToInt(t *testing.T) {
	CheckEq(t, -123, ToInt("-123"))
	CheckEq(t, -123, ToInt("-123.0"))
	CheckEq(t, -123, ToInt(int8(-123)))
	CheckEq(t, -123, ToInt(int16(-123)))
	CheckEq(t, -123, ToInt(int32(-123)))
	CheckEq(t, -123, ToInt(int64(-123)))
	CheckEq(t, 255, ToInt(uint8(255)))
	CheckEq(t, 255, ToInt(uint16(255)))
	CheckEq(t, 255, ToInt(uint32(255)))
	CheckEq(t, 255, ToInt(uint64(255)))
	CheckEq(t, -255, ToInt(float32(-255)))
	CheckEq(t, -255, ToInt(float64(-255)))
}

func TestToFloat(t *testing.T) {
	CheckEq(t, -123.0, ToFloat("-123"))
	CheckEq(t, -123.5, ToFloat("-123.5"))
	CheckEq(t, -123.0, ToFloat(int8(-123)))
	CheckEq(t, -123.0, ToFloat(int16(-123)))
	CheckEq(t, -123.0, ToFloat(int32(-123)))
	CheckEq(t, -123.0, ToFloat(int64(-123)))
	CheckEq(t, 255.0, ToFloat(uint8(255)))
	CheckEq(t, 255.0, ToFloat(uint16(255)))
	CheckEq(t, 255.0, ToFloat(uint32(255)))
	CheckEq(t, 255.0, ToFloat(uint64(255)))
	CheckEq(t, -255.5, ToFloat(float32(-255.5)))
	CheckEq(t, -255.5, ToFloat(float64(-255.5)))
}

func TestSpecialData(t *testing.T) {
	myAnyBytes := make([]byte, len(AnyBytes))
	myAnyString := string([]byte(AnyString))
	CheckFalse(t, IsAnyData(0))
	CheckFalse(t, IsAnyData(nil))
	CheckFalse(t, IsAnyData(""))
	CheckTrue(t, IsAnyData(AnyBytes))
	CheckTrue(t, IsAnyData(AnyString))
	copy(myAnyBytes, AnyBytes)
	CheckFalse(t, IsAnyData(myAnyBytes))
	CheckFalse(t, IsAnyData(myAnyString))
	CheckTrue(t, IsAnyBytes(AnyBytes))
	CheckFalse(t, IsAnyBytes(myAnyBytes))
	CheckTrue(t, IsAnyString(AnyString))
	CheckFalse(t, IsAnyString(myAnyString))
	myNilString := string([]byte(NilString))
	CheckFalse(t, IsNilData(0))
	CheckTrue(t, IsNilData(nil))
	CheckFalse(t, IsNilData(""))
	CheckTrue(t, IsNilData(NilString))
	CheckFalse(t, IsNilData(myNilString))
	CheckFalse(t, IsNilString(""))
	CheckTrue(t, IsNilString(NilString))
	CheckFalse(t, IsNilString(myNilString))
}

func TestMiscUtils(t *testing.T) {
	params := ParseParams("a=A, bb = BBB, ccc=")
	CheckEq(t, 3, len(params))
	CheckEq(t, "A", params["a"])
	CheckEq(t, "BBB", params["bb"])
	CheckEq(t, "", params["ccc"])
}

func TestStatus(t *testing.T) {
	s := NewStatus()
	CheckEq(t, StatusSuccess, s.GetCode())
	CheckEq(t, "", s.GetMessage())
	CheckTrue(t, s.Equals(s))
	CheckTrue(t, s.Equals(*s))
	CheckTrue(t, s.Equals(StatusSuccess))
	CheckFalse(t, s.Equals(StatusNotFoundError))
	CheckFalse(t, s.Equals(100))
	CheckEq(t, "SUCCESS", s)
	CheckTrue(t, s.IsOK())
	s.OrDie()
	s = NewStatus(StatusNotFoundError, "foobar")
	CheckEq(t, StatusNotFoundError, s.GetCode())
	CheckEq(t, "foobar", s.GetMessage())
	CheckEq(t, "NOT_FOUND_ERROR: foobar", s.String())
	CheckEq(t, "NOT_FOUND_ERROR", s)
	CheckEq(t, "NOT_FOUND_ERROR", s.Error())
	CheckTrue(t, s.Equals(s))
	CheckTrue(t, s.Equals(*s))
	CheckTrue(t, s.Equals(StatusNotFoundError))
	CheckFalse(t, s.Equals(StatusSuccess))
	CheckFalse(t, s.IsOK())
	CheckFalse(t, s.Equals(100))
	s = NewStatus1(StatusSuccess)
	CheckEq(t, StatusSuccess, s.GetCode())
	CheckEq(t, "", s.GetMessage())
	s = NewStatus2(StatusNotFoundError, "bazquux")
	CheckEq(t, StatusNotFoundError, s.GetCode())
	CheckEq(t, "bazquux", s.GetMessage())
	s2 := NewStatus2(StatusNotImplementedError, "void")
	s.Join(s2)
	CheckEq(t, "NOT_FOUND_ERROR: bazquux", s.String())
	s.Set(StatusSuccess, "OK")
	s.Join(s2)
	CheckEq(t, "NOT_IMPLEMENTED_ERROR: void", s.String())
	CheckEq(t, StatusSuccess, StatusSuccess)
	CheckEq(t, StatusSuccess, NewStatus1(StatusSuccess))
	CheckEq(t, NewStatus1(StatusSuccess), StatusSuccess)
	CheckEq(t, NewStatus1(StatusSuccess), NewStatus1(StatusSuccess))
	CheckEq(t, StatusNotFoundError, NewStatus1(StatusNotFoundError))
	CheckNe(t, StatusNotFoundError, StatusSuccess)
	CheckNe(t, StatusNotFoundError, NewStatus1(StatusSuccess))
	CheckNe(t, NewStatus1(StatusNotFoundError), StatusSuccess)
	CheckNe(t, NewStatus1(StatusNotFoundError), NewStatus1(StatusSuccess))
	CheckNe(t, StatusNotFoundError, NewStatus1(StatusUnknownError))
}

func TestRemoteDBM(t *testing.T) {
	dbm := NewRemoteDBM()
	CheckTrue(t, strings.Index(dbm.String(), ":unopened>") >= 0)
	status := dbm.Connect("localhost:1978", -1, "")
	CheckEq(t, StatusSuccess, status)
	CheckTrue(t, strings.Index(dbm.String(), ":opened>") >= 0)
	CheckEq(t, StatusSuccess, dbm.SetDBMIndex(-1))
	attrs := dbm.Inspect()
	CheckTrue(t, len(attrs["version"]) > 3)
	CheckTrue(t, len(attrs["num_dbms"]) > 0)
	CheckEq(t, StatusSuccess, dbm.SetDBMIndex(0))
	attrs = dbm.Inspect()
	CheckTrue(t, len(attrs["class"]) > 3)
	CheckTrue(t, len(attrs["num_records"]) > 0)
	echo, status := dbm.Echo("hello")
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "hello", echo)
	CheckEq(t, StatusSuccess, dbm.Clear())
	CheckEq(t, StatusSuccess, dbm.Set("one", "ichi", false))
	CheckEq(t, StatusDuplicationError, dbm.Set("one", "ichi", false))
	CheckEq(t, StatusSuccess, dbm.Set("one", "first", true))
	value, status := dbm.Get("one")
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "first", value)
	value, status = dbm.Get("two")
	CheckEq(t, StatusNotFoundError, status)
	strValue, status := dbm.GetStr("one")
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "first", strValue)
	value = dbm.GetSimple("one", "*")
	CheckEq(t, "first", value)
	value = dbm.GetSimple("two", "*")
	CheckEq(t, "*", value)
	CheckEq(t, "first", dbm.GetStrSimple("one", "*"))
	CheckEq(t, "*", dbm.GetStrSimple("two", "*"))
	CheckEq(t, StatusSuccess, dbm.Append("two", "second", ":"))
	CheckEq(t, StatusSuccess, dbm.Append("two", "second", ":"))
	CheckEq(t, "second:second", dbm.GetStrSimple("two", "*"))
	CheckEq(t, StatusSuccess, dbm.Remove("one"))
	CheckEq(t, StatusNotFoundError, dbm.Remove("one"))
	CheckEq(t, StatusSuccess, dbm.Set("日本", "東京", true))
	CheckEq(t, "東京", dbm.GetStrSimple("日本", "*"))
	CheckEq(t, StatusSuccess, dbm.Remove("日本"))
	records := map[string][]byte{"one": []byte("FIRST"), "two": []byte("SECOND")}
	CheckEq(t, StatusSuccess, dbm.SetMulti(records, true))
	keys := []string{"one", "two", "three"}
	records = dbm.GetMulti(keys)
	CheckEq(t, 2, len(records))
	CheckEq(t, "FIRST", records["one"])
	CheckEq(t, "SECOND", records["two"])
	strRecords := map[string]string{"one": "first", "two": "second"}
	CheckEq(t, StatusSuccess, dbm.SetMultiStr(strRecords, true))
	strRecords = dbm.GetMultiStr(keys)
	CheckEq(t, 2, len(records))
	CheckEq(t, "first", strRecords["one"])
	CheckEq(t, "second", strRecords["two"])
	CheckEq(t, StatusNotFoundError, dbm.RemoveMulti(keys))
	CheckEq(t, StatusSuccess, dbm.AppendMulti(records, ":"))
	CheckEq(t, StatusSuccess, dbm.AppendMulti(records, ":"))
	CheckEq(t, "FIRST:FIRST", dbm.GetStrSimple("one", "*"))
	CheckEq(t, "SECOND:SECOND", dbm.GetStrSimple("two", "*"))
	keys = []string{"one", "two"}
	CheckEq(t, StatusSuccess, dbm.RemoveMulti(keys))
	CheckEq(t, StatusSuccess, dbm.AppendMultiStr(strRecords, ":"))
	CheckEq(t, StatusSuccess, dbm.AppendMultiStr(strRecords, ":"))
	CheckEq(t, "first:first", dbm.GetStrSimple("one", "*"))
	CheckEq(t, "second:second", dbm.GetStrSimple("two", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchange("one", "first:first", nil))
	CheckEq(t, "*", dbm.GetSimple("one", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchange("one", nil, "hello"))
	CheckEq(t, "hello", dbm.GetSimple("one", "*"))
	CheckEq(t, StatusInfeasibleError, dbm.CompareExchange("one", nil, "hello"))
	CheckEq(t, StatusInfeasibleError, dbm.CompareExchange("xyz", AnyString, AnyString))
	CheckEq(t, StatusSuccess, dbm.CompareExchange("xyz", nil, "abc"))
	CheckEq(t, StatusSuccess, dbm.CompareExchange("xyz", AnyBytes, AnyBytes))
	CheckEq(t, "abc", dbm.GetSimple("xyz", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchange("xyz", AnyBytes, "def"))
	CheckEq(t, "def", dbm.GetSimple("xyz", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchange("xyz", AnyString, nil))
	CheckEq(t, "*", dbm.GetSimple("xyz", "*"))
	actual, status := dbm.CompareExchangeAndGetStr("xyz", nil, "123")
	CheckEq(t, StatusSuccess, status)
	CheckTrue(t, IsNilString(actual))
	actual, status = dbm.CompareExchangeAndGetStr("xyz", AnyString, AnyString)
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "123", actual)
	actual, status = dbm.CompareExchangeAndGetStr("xyz", AnyString, NilString)
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "123", actual)
	set1 := []KeyValuePair{KeyValuePair{[]byte("one"), []byte("hello")},
		KeyValuePair{[]byte("two"), []byte("second:second")}}
	set2 := []KeyValuePair{KeyValuePair{[]byte("one"), nil},
		KeyValuePair{[]byte("two"), nil}}
	set3 := []KeyValuePair{KeyValuePair{[]byte("one"), []byte("ichi")},
		KeyValuePair{[]byte("two"), []byte("ni")}}
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMulti(set1, set2))
	CheckEq(t, "*", dbm.GetSimple("one", "*"))
	CheckEq(t, "*", dbm.GetSimple("two", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMulti(set2, set3))
	CheckEq(t, "ichi", dbm.GetSimple("one", "*"))
	CheckEq(t, "ni", dbm.GetSimple("two", "*"))
	CheckEq(t, StatusInfeasibleError, dbm.CompareExchangeMulti(set2, set3))
	strSet1 := []KeyValueStrPair{KeyValueStrPair{"one", "ichi"}, KeyValueStrPair{"two", "ni"}}
	strSet2 := []KeyValueStrPair{KeyValueStrPair{"one", NilString}, KeyValueStrPair{"two", NilString}}
	strSet3 := []KeyValueStrPair{KeyValueStrPair{"one", "first"}, KeyValueStrPair{"two", "second"}}
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMultiStr(strSet1, strSet2))
	CheckEq(t, "*", dbm.GetSimple("one", "*"))
	CheckEq(t, "*", dbm.GetSimple("two", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMultiStr(strSet2, strSet3))
	CheckEq(t, "first", dbm.GetSimple("one", "*"))
	CheckEq(t, "second", dbm.GetSimple("two", "*"))
	CheckEq(t, StatusInfeasibleError, dbm.CompareExchangeMultiStr(strSet2, strSet3))
	CheckEq(t, StatusInfeasibleError, dbm.CompareExchangeMultiStr(
		[]KeyValueStrPair{{"xyz", AnyString}},
		[]KeyValueStrPair{{"xyz", "abc"}}))
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMultiStr(
		[]KeyValueStrPair{{"xyz", NilString}},
		[]KeyValueStrPair{{"xyz", "abc"}}))
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMultiStr(
		[]KeyValueStrPair{{"xyz", "abc"}},
		[]KeyValueStrPair{{"xyz", "def"}}))
	CheckEq(t, "def", dbm.GetStrSimple("xyz", "*"))
	CheckEq(t, StatusSuccess, dbm.CompareExchangeMultiStr(
		[]KeyValueStrPair{{"xyz", "def"}},
		[]KeyValueStrPair{{"xyz", NilString}}))
	CheckEq(t, "*", dbm.GetStrSimple("xyz", "*"))
	incValue, status := dbm.Increment("num", 5, 100)
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, 105, incValue)
	incValue, status = dbm.Increment("num", 5, 100)
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, 110, incValue)
	count, status := dbm.Count()
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, 3, count)
	CheckEq(t, 3, dbm.CountSimple())
	file_size, status := dbm.GetFileSize()
	if status.IsOK() {
		CheckTrue(t, file_size > 0)
		CheckEq(t, file_size, dbm.GetFileSizeSimple())
	} else {
		CheckEq(t, StatusPreconditionError, status)
		CheckEq(t, -1, dbm.GetFileSizeSimple())
	}
	CheckEq(t, StatusSuccess, dbm.Rebuild(nil))
	tobe, status := dbm.ShouldBeRebuilt()
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, tobe, dbm.ShouldBeRebuiltSimple())
	CheckEq(t, StatusSuccess, dbm.Synchronize(false, nil))
	for i := 0; i < 10; i++ {
		CheckEq(t, StatusSuccess, dbm.Set(i, i, true))
	}
	keys = dbm.Search("regex", "[23]$", 5)
	CheckEq(t, 2, len(keys))
	CheckEq(t, StatusSuccess, dbm.Disconnect())
}

func TestIterator(t *testing.T) {
	dbm := NewRemoteDBM()
	status := dbm.Connect("localhost:1978", -1, "")
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, StatusSuccess, dbm.Clear())
	for i := 0; i < 10; i++ {
		CheckEq(t, StatusSuccess, dbm.Set(i, i*i, false))
	}
	iter := dbm.MakeIterator()
	CheckTrue(t, strings.Index(iter.String(), ":opened>") >= 0)
	CheckEq(t, StatusSuccess, iter.First())
	count := 0
	for {
		key, value, status := iter.Get()
		if !status.IsOK() {
			CheckEq(t, StatusNotFoundError, status)
			break
		}
		CheckEq(t, ToInt(key)*ToInt(key), ToInt(value))
		strKey, strValue, status := iter.Get()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, ToInt(strKey)*ToInt(strKey), ToInt(strValue))
		key2, status := iter.GetKey()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, key, key2)
		strKey2, status := iter.GetKeyStr()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, strKey, strKey2)
		value2, status := iter.GetValue()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, value, value2)
		strValue2, status := iter.GetValueStr()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, strValue, strValue2)
		CheckEq(t, StatusSuccess, iter.Next())
		count += 1
	}
	CheckEq(t, dbm.CountSimple(), count)
	for i := 0; i < count; i++ {
		CheckEq(t, StatusSuccess, iter.Jump(i))
		key, value, status := iter.Get()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, i, ToInt(key))
		CheckEq(t, i*i, ToInt(value))
	}
	count = 0
	for record := range dbm.Each() {
		CheckEq(t, ToInt(record.Key)*ToInt(record.Key), ToInt(record.Value))
		count += 1
	}
	CheckEq(t, dbm.CountSimple(), count)
	count = 0
	for record := range dbm.EachStr() {
		CheckEq(t, ToInt(record.Key)*ToInt(record.Key), ToInt(record.Value))
		count += 1
	}
	CheckEq(t, dbm.CountSimple(), count)
	status = iter.Last()
	if status.IsOK() {
		count = 0
		for {
			key, value, status := iter.Get()
			if !status.IsOK() {
				CheckEq(t, StatusNotFoundError, status)
				break
			}
			CheckEq(t, ToInt(key)*ToInt(key), ToInt(value))
			CheckEq(t, StatusSuccess, iter.Previous())
			count += 1
		}
		CheckEq(t, dbm.CountSimple(), count)
		CheckEq(t, StatusSuccess, iter.JumpLower("0", false))
		key, status := iter.GetKey()
		CheckEq(t, StatusNotFoundError, status)
		CheckTrue(t, key == nil)
		CheckEq(t, StatusSuccess, iter.JumpLower("0", true))
		key, status = iter.GetKey()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, "0", key)
		CheckEq(t, StatusSuccess, iter.Next())
		key, status = iter.GetKey()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, "1", key)
		CheckEq(t, StatusSuccess, iter.JumpUpper("9", false))
		key, status = iter.GetKey()
		CheckEq(t, StatusNotFoundError, status)
		CheckTrue(t, key == nil)
		CheckEq(t, StatusSuccess, iter.JumpUpper("9", true))
		key, status = iter.GetKey()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, "9", key)
		CheckEq(t, StatusSuccess, iter.Previous())
		key, status = iter.GetKey()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, "8", key)
		CheckEq(t, StatusSuccess, iter.Set("eight"))
		value, status := iter.GetValue()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, "eight", value)
		CheckEq(t, StatusSuccess, iter.Remove())
		key, status = iter.GetKey()
		CheckEq(t, StatusSuccess, status)
		CheckEq(t, "9", key)
		CheckEq(t, StatusSuccess, iter.Remove())
		CheckEq(t, StatusNotFoundError, iter.Remove())
		CheckEq(t, 8, dbm.CountSimple())
	} else {
		CheckEq(t, StatusNotImplementedError, status)
	}
	CheckEq(t, StatusSuccess, dbm.Clear())
	CheckEq(t, StatusSuccess, dbm.PushLast("one", 0, false))
	CheckEq(t, StatusSuccess, dbm.PushLast("two", 0, true))
	CheckEq(t, StatusSuccess, iter.First())
	key, value, status := iter.Step()
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "\x00\x00\x00\x00\x00\x00\x00\x00", key)
	CheckEq(t, "one", value)
	strKey, strValue, status := iter.StepStr()
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "\x00\x00\x00\x00\x00\x00\x00\x01", strKey)
	CheckEq(t, "two", strValue)
	strKey, strValue, status = iter.StepStr()
	CheckEq(t, StatusNotFoundError, status)
	key, value, status = dbm.PopFirst(0)
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "\x00\x00\x00\x00\x00\x00\x00\x00", key)
	CheckEq(t, "one", value)
	strKey, strValue, status = dbm.PopFirstStr(0)
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, "\x00\x00\x00\x00\x00\x00\x00\x01", strKey)
	CheckEq(t, "two", strValue)
	strKey, strValue, status = dbm.PopFirstStr(0)
	CheckEq(t, StatusNotFoundError, status)
	iter.Destruct()
	CheckEq(t, StatusSuccess, dbm.Disconnect())
}

func TestThread(t *testing.T) {
	dbm := NewRemoteDBM()
	status := dbm.Connect("localhost:1978", -1, "")
	CheckEq(t, StatusSuccess, status)
	CheckEq(t, StatusSuccess, dbm.Clear())
	numIterations := 1000
	numThreads := 5
	recordMaps := make([]map[string]string, 0, numThreads)
	mutexes := make([]sync.Mutex, 0, numThreads)
	for i := 0; i < numThreads; i++ {
		recordMaps = append(recordMaps, make(map[string]string))
		mutexes = append(mutexes, sync.Mutex{})
	}
	task := func(thid int, done chan<- bool) {
		random := rand.New(rand.NewSource(int64(thid)))
		for i := 0; i < numIterations; i++ {
			keyNum := random.Intn(numIterations * numThreads)
			valueNum := random.Intn(numIterations * numThreads)
			key := fmt.Sprintf("%d", keyNum)
			value := fmt.Sprintf("%d", valueNum*valueNum)
			groupIndex := keyNum % numThreads
			recordMap := &recordMaps[groupIndex]
			mutex := &mutexes[groupIndex]
			mutex.Lock()
			if random.Intn(5) == 0 {
				gotValue, status := dbm.Get(key)
				if status.IsOK() {
					CheckEq(t, (*recordMap)[key], gotValue)
				} else {
					CheckEq(t, StatusNotFoundError, status)
				}
			} else if random.Intn(5) == 0 {
				status := dbm.Remove(key)
				CheckTrue(t, status.Equals(StatusSuccess) || status.Equals(StatusNotFoundError))
				delete(*recordMap, key)
			} else {
				CheckEq(t, StatusSuccess, dbm.Set(key, value, true))
				(*recordMap)[key] = value
			}
			mutex.Unlock()
			if random.Intn(10) == 0 {
				iter := dbm.MakeIterator()
				iter.Jump(key)
				_, _, status := iter.Get()
				CheckTrue(t, status.Equals(StatusSuccess) || status.Equals(StatusNotFoundError))
				iter.Destruct()
			}
		}
		done <- true
	}
	dones := make([]chan bool, 0)
	for i := 0; i < numThreads; i++ {
		done := make(chan bool)
		go task(i, done)
		dones = append(dones, done)
	}
	for _, done := range dones {
		<-done
	}
	numRecords := 0
	for _, recordMap := range recordMaps {
		numRecords += len(recordMap)
		for key, value := range recordMap {
			gotValue, status := dbm.Get(key)
			CheckEq(t, StatusSuccess, status)
			CheckEq(t, value, gotValue)
		}
	}
	CheckEq(t, numRecords, dbm.CountSimple())
	CheckEq(t, StatusSuccess, dbm.Disconnect())
}

// END OF FILE
