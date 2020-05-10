package likepay_pg

import (
	"database/sql/driver"
	"time"
)

type (
	MaybeInterface interface {
		IsNull() bool
		IsOmitted() bool
	}
	/* all of these types must have both of implementation:
	   sql/driver.Valuer and MaybeInterface
	*/
	MaybeBase struct {
		valid bool
		set   bool
	}
	MaybeTime struct {
		MaybeBase
		value   time.Time
	}
	MaybeString struct {
		MaybeBase
		value   string
	}
	MaybeBool struct {
		MaybeBase
		value   bool
	}
	MaybeInt struct {
		MaybeBase
		value   int
	}
	MaybeInt8 struct {
		MaybeBase
		value   int8
	}
	MaybeInt16 struct {
		MaybeBase
		value   int16
	}
	MaybeInt32 struct {
		MaybeBase
		value   int32
	}
	MaybeInt64 struct {
		MaybeBase
		value   int64
	}
	MaybeUInt struct {
		MaybeBase
		value   uint
	}
	MaybeUInt8 struct {
		MaybeBase
		value   uint8
	}
	MaybeUInt16 struct {
		MaybeBase
		value   uint16
	}
	MaybeUInt32 struct {
		MaybeBase
		value   uint32
	}
	MaybeUInt64 struct {
		MaybeBase
		value   uint64
	}
	MaybeFloat32 struct {
		MaybeBase
		value   float32
	}
	MaybeFloat64 struct {
		MaybeBase
		value   float64
	}
)

/* sql/driver.Valuer interface implementations */

func (c *MaybeTime) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeString) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeBool) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeInt) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeInt8) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeInt16) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeInt32) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeInt64) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeUInt) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeUInt8) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeUInt16) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeUInt32) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeUInt64) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeFloat32) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

func (c *MaybeFloat64) Value() (driver.Value, error) {
	if !c.valid {
		return nil, nil
	}
	return c.value, nil
}

/* MaybeInterface interface implementations */

func (c *MaybeBase) IsNull() bool {
	return !c.valid
}

func (c *MaybeBase) IsOmitted() bool {
	return !c.set
}

/* makers */

var (
	/*
		expression is Null when value of the `valid` field is false
		the valuer omit when value of the `set` field is false
		so we have three flag combinations:
	*/
	nullMaybe = MaybeBase{
		valid: false, // implicit null
		set:   true,
	} /* for nulls */
	setMaybe = MaybeBase{
		valid: true,
		set:   true,
	} /* for valid value */
	/* and defaults that do not need an explicit declaration
		omitMaybe = MaybeBase{
			valid: false,
			set:   false,
		}
	*/
)

func MaybeTimeSet(t time.Time) MaybeTime {
	return MaybeTime{
		MaybeBase: setMaybe,
		value:     t,
	}
}

func MaybeTimeNull() MaybeTime {
	return MaybeTime{
		MaybeBase: nullMaybe,
	}
}

func MaybeStringSet(s string) MaybeString {
	return MaybeString{
		MaybeBase: setMaybe,
		value:     s,
	}
}

func MaybeStringNull() MaybeString {
	return MaybeString{
		MaybeBase: nullMaybe,
	}
}

func MaybeBoolSet(b bool) MaybeBool {
	return MaybeBool{
		MaybeBase: setMaybe,
		value:     b,
	}
}

func MaybeBoolNull() MaybeBool {
	return MaybeBool{
		MaybeBase: nullMaybe,
	}
}

func MaybeIntSet(i int) MaybeInt {
	return MaybeInt{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeIntNull() MaybeInt {
	return MaybeInt{
		MaybeBase: nullMaybe,
	}
}

func MaybeInt8Set(i int8) MaybeInt8 {
	return MaybeInt8{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeInt8Null() MaybeInt8 {
	return MaybeInt8{
		MaybeBase: nullMaybe,
	}
}

func MaybeInt16Set(i int16) MaybeInt16 {
	return MaybeInt16{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeInt16Null() MaybeInt16 {
	return MaybeInt16{
		MaybeBase: nullMaybe,
	}
}

func MaybeInt32Set(i int32) MaybeInt32 {
	return MaybeInt32{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeInt32Null() MaybeInt32 {
	return MaybeInt32{
		MaybeBase: nullMaybe,
	}
}

func MaybeInt64Set(i int64) MaybeInt64 {
	return MaybeInt64{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeInt64Null() MaybeInt64 {
	return MaybeInt64{
		MaybeBase: nullMaybe,
	}
}

func MaybeUIntSet(i uint) MaybeUInt {
	return MaybeUInt{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeUIntNull() MaybeUInt {
	return MaybeUInt{
		MaybeBase: nullMaybe,
	}
}

func MaybeUInt8Set(i uint8) MaybeUInt8 {
	return MaybeUInt8{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeUInt8Null() MaybeUInt8 {
	return MaybeUInt8{
		MaybeBase: nullMaybe,
	}
}

func MaybeUInt16Set(i uint16) MaybeUInt16 {
	return MaybeUInt16{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeUInt16Null() MaybeUInt16 {
	return MaybeUInt16{
		MaybeBase: nullMaybe,
	}
}

func MaybeUInt32Set(i uint32) MaybeUInt32 {
	return MaybeUInt32{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeUInt32Null() MaybeUInt32 {
	return MaybeUInt32{
		MaybeBase: nullMaybe,
	}
}

func MaybeUInt64Set(i uint64) MaybeUInt64 {
	return MaybeUInt64{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeUInt64Null() MaybeUInt64 {
	return MaybeUInt64{
		MaybeBase: nullMaybe,
	}
}

func MaybeFloat32Set(i float32) MaybeFloat32 {
	return MaybeFloat32{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeFloat32Null() MaybeFloat32 {
	return MaybeFloat32{
		MaybeBase: nullMaybe,
	}
}

func MaybeFloat64Set(i float64) MaybeFloat64 {
	return MaybeFloat64{
		MaybeBase: setMaybe,
		value:     i,
	}
}

func MaybeFloat64Null() MaybeFloat64 {
	return MaybeFloat64{
		MaybeBase: nullMaybe,
	}
}
